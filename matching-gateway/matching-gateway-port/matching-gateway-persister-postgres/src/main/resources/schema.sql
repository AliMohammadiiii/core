CREATE TABLE IF NOT EXISTS pair_setting
(
    pair         VARCHAR(72) PRIMARY KEY,
    is_available BOOLEAN NOT NULL DEFAULT FALSE,
    update_date  TIMESTAMP
);

DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1
                       FROM information_schema.columns
                       WHERE table_name = 'pair_setting' AND column_name = 'min_order') THEN ALTER TABLE pair_setting
            ADD COLUMN min_order DECIMAL NOT NULL default 1;
        END IF;
        IF NOT EXISTS (SELECT 1
                       FROM information_schema.columns
                       WHERE table_name = 'pair_setting' AND column_name = 'max_order') THEN ALTER TABLE pair_setting
            ADD COLUMN max_order DECIMAL NOT NULL default 1;
        END IF;
        IF NOT EXISTS (SELECT 1
                       FROM information_schema.columns
                       WHERE table_name = 'pair_setting' AND column_name = 'order_types') THEN ALTER TABLE pair_setting
            ADD COLUMN order_types varchar(255) NOT NULL default 'LIMIT_ORDER, MARKET_ORDER' ;
        END IF;
        IF NOT EXISTS (SELECT 1
                       FROM information_schema.columns
                       WHERE table_name = 'pair_setting'
                         AND column_name = 'internal_chart') THEN ALTER TABLE pair_setting
            ADD COLUMN internal_chart BOOLEAN NOT NULL default true;
        END IF;
        IF NOT EXISTS (SELECT 1
                       FROM information_schema.columns
                       WHERE table_name = 'pair_setting' AND column_name = 'global_chart') THEN ALTER TABLE pair_setting
            ADD COLUMN global_chart BOOLEAN NOT NULL default true;
        END IF;
    END
$$;

CREATE TABLE IF NOT EXISTS pair_category
(
    id       SERIAL PRIMARY KEY,
    pair     VARCHAR(72)  NOT NULL REFERENCES pair_setting (pair),
    category VARCHAR(255) NOT NULL,
    UNIQUE (pair, category)
);

-- Additive durable admission contract. Legacy matching orders are not rewritten.
CREATE TABLE IF NOT EXISTS order_intent (
    ouid VARCHAR(72) PRIMARY KEY,
    owner VARCHAR(72) NOT NULL,
    client_order_id VARCHAR(80) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    symbol VARCHAR(72) NOT NULL,
    order_type VARCHAR(20) NOT NULL,
    side VARCHAR(8) NOT NULL,
    quantity DECIMAL NOT NULL CHECK (quantity > 0),
    price DECIMAL NOT NULL CHECK (price >= 0),
    stop_price DECIMAL CHECK (stop_price IS NULL OR stop_price > 0),
    post_only BOOLEAN NOT NULL DEFAULT FALSE,
    left_fraction DECIMAL NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'ACCEPTED_PENDING',
    order_id BIGINT,
    filled_quantity DECIMAL NOT NULL DEFAULT 0,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    reason VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (owner, client_order_id),
    CHECK (filled_quantity >= 0 AND filled_quantity <= quantity)
);
ALTER TABLE order_intent ADD COLUMN IF NOT EXISTS stop_price DECIMAL;
CREATE TABLE IF NOT EXISTS order_intent_outbox (
    id BIGSERIAL PRIMARY KEY,
    ouid VARCHAR(72) NOT NULL REFERENCES order_intent(ouid),
    kind VARCHAR(10) NOT NULL,
    payload TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    UNIQUE (ouid, kind)
);
CREATE INDEX IF NOT EXISTS order_intent_outbox_pending ON order_intent_outbox(id) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS order_intent_event (
    cursor BIGSERIAL PRIMARY KEY,
    event_key VARCHAR(200) NOT NULL UNIQUE,
    owner VARCHAR(72) NOT NULL,
    ouid VARCHAR(72) NOT NULL REFERENCES order_intent(ouid),
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS order_intent_event_owner_cursor ON order_intent_event(owner, cursor);
