import os
import pandas as pd

from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from psycopg2.extras import execute_batch

GLOBAL_PATH = "/opt/airflow/dataset"

# Получаем соединение с PostgreSQL.
def _get_pg_connection():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    return hook.get_conn()

# Записываем пользователей в базу данных.
def upsert_users():
    with _get_pg_connection() as conn:
        with conn.cursor() as cursor:
            for name in os.listdir(GLOBAL_PATH):
                df = pd.read_parquet(f"{GLOBAL_PATH}/{name}")
                data = df[["user_id", "user_phone"]].drop_duplicates()
                data = list(data.itertuples(index=False, name=None))
                execute_batch(cursor, """
                                      INSERT INTO users (user_id, user_phone)
                                      VALUES (%s, %s) ON CONFLICT(user_id) 
                                      DO UPDATE SET user_phone = EXCLUDED.user_phone;
                                      """.strip(), data)

        # Коммитим все изменения.
        conn.commit()

# Записываем курьеров в базу данных.
def upsert_drivers():
    with _get_pg_connection() as conn:
        with conn.cursor() as cursor:
            for name in os.listdir(GLOBAL_PATH):
                df = pd.read_parquet(f"{GLOBAL_PATH}/{name}")
                data = df[["driver_id", "driver_phone"]].drop_duplicates()
                data = list(data.itertuples(index=False, name=None))
                execute_batch(cursor, """
                                      INSERT INTO drivers (driver_id, driver_phone)
                                      VALUES (%s, %s) ON CONFLICT(driver_id)
                                      DO
                                      UPDATE SET driver_phone = EXCLUDED.driver_phone;
                                      """.strip(), data)

        # Коммитим все изменения.
        conn.commit()

# Записываем адресы в базу данных.
def upsert_addresses():
    with _get_pg_connection() as conn:
        with conn.cursor() as cursor:
            for name in os.listdir(GLOBAL_PATH):
                df = pd.read_parquet(f"{GLOBAL_PATH}/{name}")
                delivery_addresses, store_addresses = df[["address_text"]], df[["store_address"]]
                delivery_addresses = delivery_addresses.rename(columns={'address_text': 'address'})
                store_addresses = store_addresses.rename(columns={'store_address': 'address'})
                addresses = pd.concat([delivery_addresses, store_addresses], ignore_index=True).drop_duplicates()
                data = list(addresses.itertuples(index=False, name=None))
                execute_batch(cursor, """
                                      INSERT INTO addresses (address)
                                      VALUES (%s) ON CONFLICT(address) DO NOTHING;
                                      """.strip(), data)

        # Коммитим все изменения.
        conn.commit()

# Записываем категории в базу данных.
def upsert_categories():
    with _get_pg_connection() as conn:
        with conn.cursor() as cursor:
            for name in os.listdir(GLOBAL_PATH):
                df = pd.read_parquet(f"{GLOBAL_PATH}/{name}")
                data = list(df[["item_category"]].drop_duplicates().itertuples(index=False, name=None))
                execute_batch(cursor, """
                                      INSERT INTO categories (category_name)
                                      VALUES (%s) ON CONFLICT(category_name) DO NOTHING;
                                      """.strip(), data)

        # Коммитим все изменения.
        conn.commit()

# Записываем магазины в базу данных.
def upsert_stores():
    with _get_pg_connection() as conn:
        with conn.cursor() as cursor:
            for name in os.listdir(GLOBAL_PATH):
                df = pd.read_parquet(f"{GLOBAL_PATH}/{name}")

                # Создаем и заполняем временную таблицу.
                cursor.execute("CREATE TEMP TABLE tmp_stores (store_id BIGINT, store_address TEXT);")
                data = list(df[["store_id", "store_address"]].drop_duplicates().itertuples(index=False, name=None))
                execute_batch(cursor, "INSERT INTO tmp_stores (store_id, store_address) VALUES (%s, %s);", data)

                # Затем переносим все из временной таблицы в основную, заменяя адреса на их ID.
                cursor.execute("""
                               INSERT INTO stores (store_id, store_address_id)
                               SELECT tmp.store_id, a.address_id
                               FROM tmp_stores tmp
                               JOIN addresses a ON tmp.store_address = a.address 
                               ON CONFLICT(store_id) DO UPDATE
                               SET store_address_id = EXCLUDED.store_address_id;
                               """.strip())

                # Убираем временную таблицу.
                cursor.execute("DROP TABLE tmp_stores;")

        # Коммитим все изменения.
        conn.commit()

# Записываем товары в базу данных.
def upsert_items():
    with _get_pg_connection() as conn:
        with conn.cursor() as cursor:
            for name in os.listdir(GLOBAL_PATH):
                df = pd.read_parquet(f"{GLOBAL_PATH}/{name}")

                # Создаем и заполняем временную таблицу.
                cursor.execute("CREATE TEMP TABLE tmp_items (item_id BIGINT, item_title TEXT, item_category TEXT);")
                data = list(
                    df[["item_id", "item_title", "item_category"]].drop_duplicates().itertuples(index=False, name=None))
                execute_batch(cursor, "INSERT INTO tmp_items (item_id, item_title, item_category) VALUES (%s, %s, %s);",
                              data)

                # Затем переносим все в основную таблицу, заменяя категорию на ее ID.
                cursor.execute("""
                               INSERT INTO items (item_id, item_title, category_id)
                               SELECT tmp.item_id, tmp.item_title, c.category_id
                               FROM tmp_items tmp
                               JOIN categories c ON tmp.item_category = c.category_name 
                               ON CONFLICT(item_id) DO UPDATE
                               SET item_title = EXCLUDED.item_title,
                                   category_id = EXCLUDED.category_id;
                               """.strip())

                # Убираем временную таблицу.
                cursor.execute("DROP TABLE tmp_items;")

        # Коммитим все изменения.
        conn.commit()

# Записываем заказы в базу данных.
def upsert_orders():
    with _get_pg_connection() as conn:
        with conn.cursor() as cursor:
            for name in os.listdir(GLOBAL_PATH):
                df = pd.read_parquet(f"{GLOBAL_PATH}/{name}")
                df = df.replace({pd.NA: None, pd.NaT: None})

                # Создаем временную таблицу для заказов
                cursor.execute("""
                               CREATE
                               TEMP TABLE tmp_orders (
                                        order_id BIGINT,
                                        user_id BIGINT,
                                        store_id BIGINT,
                                        address_text TEXT,
                                        created_at TIMESTAMP,
                                        paid_at TIMESTAMP,
                                        payment_type TEXT,
                                        order_discount DECIMAL,
                                        order_cancellation_reason TEXT
                                    );
                               """.strip())

                # Заполняем временную таблицу.
                data = list(df[[
                    "order_id",
                    "user_id",
                    "store_id",
                    "address_text",
                    "created_at",
                    "paid_at",
                    "payment_type",
                    "order_discount",
                    "order_cancellation_reason"
                ]].drop_duplicates().itertuples(index=False, name=None))
                execute_batch(cursor, """
                                      INSERT INTO tmp_orders (order_id, user_id, store_id, address_text, created_at,
                                                              paid_at,
                                                              payment_type, order_discount, order_cancellation_reason)
                                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                                      """.strip(), data)

                # Переносим все в основную таблицу.
                cursor.execute("""
                               INSERT INTO orders (order_id, user_id, store_id, address_id, created_at, paid_at,
                                                   payment_type, order_discount, order_cancellation_reason)
                               SELECT tmp.order_id,
                                      tmp.user_id,
                                      tmp.store_id,
                                      a.address_id,
                                      tmp.created_at,
                                      tmp.paid_at,
                                      tmp.payment_type,
                                      tmp.order_discount,
                                      tmp.order_cancellation_reason
                               FROM tmp_orders tmp
                                        JOIN addresses a ON tmp.address_text = a.address ON CONFLICT(order_id) DO
                               UPDATE
                                   SET user_id = EXCLUDED.user_id,
                                   store_id = EXCLUDED.store_id,
                                   address_id = EXCLUDED.address_id,
                                   created_at = EXCLUDED.created_at,
                                   paid_at = EXCLUDED.paid_at,
                                   payment_type = EXCLUDED.payment_type,
                                   order_discount = EXCLUDED.order_discount,
                                   order_cancellation_reason = EXCLUDED.order_cancellation_reason;
                               """.strip())

                # Убираем временную таблицу.
                cursor.execute("DROP TABLE tmp_orders;")

        # Коммитим все изменения.
        conn.commit()

def upsert_order_items():
    with _get_pg_connection() as conn:
        with conn.cursor() as cursor:
            for name in os.listdir(GLOBAL_PATH):
                df = pd.read_parquet(f"{GLOBAL_PATH}/{name}")
                df = df.replace({pd.NA: None, pd.NaT: None})
                df["item_replaced_id"] = df["item_replaced_id"].astype("Int64")
                data = df[["order_id", "item_id", "item_quantity", "item_price", "item_canceled_quantity",
                           "item_replaced_id", "item_discount"]]
                data = [
                    tuple(None if pd.isna(x) else int(x) for x in row)
                    for row in data.itertuples(index=False, name=None)
                ]

                # Добавляем данные в базу данных
                execute_batch(cursor, """
                                      INSERT INTO order_items (order_id, item_id, item_quantity, item_price,
                                                               item_canceled_quantity, item_replaced_id, item_discount)
                                      VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT(order_id, item_id) DO
                                      UPDATE SET
                                          item_quantity = EXCLUDED.item_quantity,
                                          item_price = EXCLUDED.item_price,
                                          item_canceled_quantity = EXCLUDED.item_canceled_quantity,
                                          item_replaced_id = EXCLUDED.item_replaced_id,
                                          item_discount = EXCLUDED.item_discount;
                                      """.strip(), data)

        # Коммитим все изменения.
        conn.commit()

def upsert_deliveries():
    with _get_pg_connection() as conn:
        with conn.cursor() as cursor:
            for name in os.listdir(GLOBAL_PATH):
                df = pd.read_parquet(f"{GLOBAL_PATH}/{name}")
                df['driver_id'] = df['driver_id'].where(df['driver_id'].notna(), None)
                df = df.replace({pd.NA: None, pd.NaT: None})
                data = df[["order_id", "driver_id", "delivery_started_at", "delivered_at", "canceled_at"]]
                data = list(data.drop_duplicates().itertuples(index=False, name=None))

                # Добавляем данные в базу данных
                execute_batch(cursor, """
                                      INSERT INTO deliveries (order_id, driver_id, started_at, finished_at,
                                                              cancelled_at)
                                      VALUES (%s, %s, %s, %s, %s) ON CONFLICT(order_id, driver_id, started_at) DO
                                      UPDATE SET
                                          driver_id = EXCLUDED.driver_id,
                                          finished_at = EXCLUDED.finished_at,
                                          cancelled_at = EXCLUDED.cancelled_at;
                                      """.strip(), data)

        # Коммитим все изменения.
        conn.commit()

with DAG(dag_id="deliveries_replication", start_date=datetime(2026, 3, 8)) as dag:
    begin, end = EmptyOperator(task_id="begin"), EmptyOperator(task_id="end")
    between_step = EmptyOperator(task_id="between_step")

    # Создаем таски.
    users_upsert_task = PythonOperator(task_id="users_upsert", python_callable=upsert_users)
    drivers_upsert_task = PythonOperator(task_id="drivers_upsert", python_callable=upsert_drivers)
    addresses_upsert_task = PythonOperator(task_id="addresses_upsert", python_callable=upsert_addresses)
    categories_upsert_task = PythonOperator(task_id="categories_upsert", python_callable=upsert_categories)
    stores_upsert_task = PythonOperator(task_id="stores_upsert", python_callable=upsert_stores)
    items_upsert_task = PythonOperator(task_id="items_upsert", python_callable=upsert_items)
    orders_upsert_task = PythonOperator(task_id="orders_upsert", python_callable=upsert_orders)
    order_items_upsert_task = PythonOperator(task_id="order_items_upsert", python_callable=upsert_order_items)
    deliveries_upsert_task = PythonOperator(task_id="deliveries_upsert", python_callable=upsert_deliveries)

    # Строим пайплайн.
    begin >> [users_upsert_task, drivers_upsert_task, addresses_upsert_task, categories_upsert_task]
    addresses_upsert_task >> stores_upsert_task
    categories_upsert_task >> items_upsert_task
    [users_upsert_task, stores_upsert_task, addresses_upsert_task] >> orders_upsert_task
    [orders_upsert_task, items_upsert_task] >> order_items_upsert_task
    [orders_upsert_task, drivers_upsert_task] >> deliveries_upsert_task
    [order_items_upsert_task, deliveries_upsert_task] >> end
