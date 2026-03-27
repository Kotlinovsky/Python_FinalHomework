-- Создадим таблицу для пользователей, чтобы не дублировать информацию о пользователях в заказах.
CREATE TABLE users
(
    user_id    BIGINT PRIMARY KEY,
    user_phone TEXT
);

-- Создадим таблицу для адресов доставки, чтобы не дублировать названия адресов в заказах.
-- Также отдельная таблица позволит более удобно обрабатывать переименование, например, проектируемых проездов в Москве.
CREATE TABLE addresses
(
    address_id BIGSERIAL PRIMARY KEY,
    address    TEXT UNIQUE
);

-- Создадим таблицу для категорий товаров, чтобы менять название категорий централизованно и не дублировать их названия.
CREATE TABLE categories
(
    category_id   BIGSERIAL PRIMARY KEY,
    category_name TEXT UNIQUE
);

-- Создадим таблицу для доставщиков, чтобы не дублировать их данные в заказах.
CREATE TABLE drivers
(
    driver_id    BIGINT PRIMARY KEY,
    driver_phone TEXT
);

-- Создадим таблицу для магазинов, так как эта отдельная сущность от заказа.
CREATE TABLE stores
(
    store_id         BIGINT PRIMARY KEY,
    store_address_id BIGINT REFERENCES addresses (address_id)
);

-- Создадим таблицу для товаров, поскольку они могут существовать и без заказа.
CREATE TABLE items
(
    item_id     BIGINT PRIMARY KEY,
    category_id BIGINT REFERENCES categories (category_id),
    item_title  TEXT
);

-- Создадим таблицу для заказов.
CREATE TABLE orders
(
    order_id                  BIGINT PRIMARY KEY,
    user_id                   BIGINT REFERENCES users (user_id),
    store_id                  BIGINT REFERENCES stores (store_id),
    address_id                BIGINT REFERENCES addresses (address_id),
    created_at                TIMESTAMP,
    paid_at                   TIMESTAMP,
    payment_type              TEXT,
    order_discount            DECIMAL,
    order_cancellation_reason TEXT
);

-- Создадим таблицу для покупок внутри заказа.
CREATE TABLE order_items
(
    order_item_id          BIGSERIAL PRIMARY KEY,
    order_id               BIGINT REFERENCES orders (order_id),
    item_id                BIGINT REFERENCES items (item_id),
    item_quantity          INT,
    item_price             DECIMAL,
    item_canceled_quantity INT,
    item_replaced_id       BIGINT REFERENCES items (item_id),
    item_discount          DECIMAL
);

-- Создадим таблицу для доставок. Отдельная таблица позволит обрабатывать несколько доставок в рамках одного заказа.
CREATE TABLE deliveries
(
    delivery_id  BIGSERIAL PRIMARY KEY,
    order_id     BIGINT REFERENCES orders (order_id),
    driver_id    BIGINT REFERENCES drivers (driver_id),
    started_at   TIMESTAMP,
    finished_at  TIMESTAMP,
    cancelled_at TIMESTAMP
);
