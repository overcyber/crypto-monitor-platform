CREATE DATABASE IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.events_hot
(
    schema_version UInt16,
    event_id String,
    source_id String,
    venue LowCardinality(String),
    channel LowCardinality(String),
    event_type LowCardinality(String),
    symbol LowCardinality(String),
    sequence_id Nullable(Int64),
    first_sequence_id Nullable(Int64),
    event_time DateTime64(3, 'UTC'),
    event_time_ms Int64,
    ingest_time_ms Int64,
    price Nullable(Float64),
    quantity Nullable(Float64),
    side Nullable(String),
    bid_price Nullable(Float64),
    bid_quantity Nullable(Float64),
    ask_price Nullable(Float64),
    ask_quantity Nullable(Float64)
)
ENGINE = ReplacingMergeTree(ingest_time_ms)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (venue, symbol, event_type, event_time, event_id)
TTL event_time + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS market.cleaned_kafka
(
    schema_version UInt16,
    event_id String,
    source_id String,
    venue String,
    channel String,
    event_type String,
    symbol String,
    sequence_id Nullable(Int64),
    first_sequence_id Nullable(Int64),
    event_time_ms Int64,
    ingest_time_ms Int64,
    price Nullable(Float64),
    quantity Nullable(Float64),
    side Nullable(String),
    bid_price Nullable(Float64),
    bid_quantity Nullable(Float64),
    ask_price Nullable(Float64),
    ask_quantity Nullable(Float64),
    bids_json String,
    asks_json String,
    raw_payload String,
    metadata_json String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'market.cleaned',
    kafka_group_name = 'clickhouse-hot-v4',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 4,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS market.cleaned_to_hot
TO market.events_hot
AS
SELECT
    schema_version, event_id, source_id, venue, channel, event_type, symbol,
    sequence_id, first_sequence_id,
    fromUnixTimestamp64Milli(event_time_ms) AS event_time,
    event_time_ms, ingest_time_ms, price, quantity, side,
    bid_price, bid_quantity, ask_price, ask_quantity
FROM market.cleaned_kafka;


CREATE TABLE IF NOT EXISTS market.candles_1m
(
    venue LowCardinality(String),
    symbol LowCardinality(String),
    open_time DateTime64(3, 'UTC'),
    open_time_ms Int64,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    trade_count UInt64,
    last_event_time_ms Int64,
    version_ms Int64
)
ENGINE = ReplacingMergeTree(version_ms)
PARTITION BY toYYYYMM(open_time)
ORDER BY (venue, symbol, open_time)
TTL open_time + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS market.microstructure_features
(
    feature_id String,
    source_event_id String,
    venue LowCardinality(String),
    symbol LowCardinality(String),
    event_time DateTime64(3, 'UTC'),
    event_time_ms Int64,
    ingest_time_ms Int64,
    best_bid Nullable(Float64),
    best_ask Nullable(Float64),
    mid Nullable(Float64),
    spread Nullable(Float64),
    spread_bps Nullable(Float64),
    microprice Nullable(Float64),
    microprice_offset_bps Nullable(Float64),
    imbalance_l1 Nullable(Float64),
    imbalance_l5 Nullable(Float64),
    imbalance_l10 Nullable(Float64),
    imbalance_l20 Nullable(Float64),
    weighted_imbalance Nullable(Float64),
    bid_depth_10bps Nullable(Float64),
    ask_depth_10bps Nullable(Float64),
    ofi Nullable(Float64),
    tfi_60s Nullable(Float64),
    metadata_json String
)
ENGINE = ReplacingMergeTree(ingest_time_ms)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (venue, symbol, event_time, feature_id)
TTL event_time + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS market.microstructure_kafka
(
    feature_id String,
    source_event_id String,
    venue String,
    symbol String,
    event_time_ms Int64,
    ingest_time_ms Int64,
    best_bid Nullable(Float64),
    best_ask Nullable(Float64),
    mid Nullable(Float64),
    spread Nullable(Float64),
    spread_bps Nullable(Float64),
    microprice Nullable(Float64),
    microprice_offset_bps Nullable(Float64),
    imbalance_l1 Nullable(Float64),
    imbalance_l5 Nullable(Float64),
    imbalance_l10 Nullable(Float64),
    imbalance_l20 Nullable(Float64),
    weighted_imbalance Nullable(Float64),
    bid_depth_10bps Nullable(Float64),
    ask_depth_10bps Nullable(Float64),
    ofi Nullable(Float64),
    tfi_60s Nullable(Float64),
    metadata_json String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'market.microstructure',
    kafka_group_name = 'clickhouse-microstructure-v4',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS market.microstructure_to_hot
TO market.microstructure_features
AS
SELECT
    feature_id, source_event_id, venue, symbol,
    fromUnixTimestamp64Milli(event_time_ms) AS event_time,
    event_time_ms, ingest_time_ms,
    best_bid, best_ask, mid, spread, spread_bps,
    microprice, microprice_offset_bps,
    imbalance_l1, imbalance_l5, imbalance_l10, imbalance_l20,
    weighted_imbalance, bid_depth_10bps, ask_depth_10bps,
    ofi, tfi_60s, metadata_json
FROM market.microstructure_kafka;

CREATE TABLE IF NOT EXISTS market.reconciliation_results
(
    run_id String,
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    venue LowCardinality(String),
    symbol LowCardinality(String),
    event_type LowCardinality(String),
    raw_rows UInt64,
    raw_unique_events UInt64,
    raw_quantity Float64,
    raw_notional Float64,
    hot_rows UInt64,
    hot_unique_events UInt64,
    hot_quantity Float64,
    hot_notional Float64,
    status LowCardinality(String),
    details_json String,
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, run_id, venue, symbol, event_type)
TTL created_at + INTERVAL 365 DAY DELETE;

CREATE VIEW IF NOT EXISTS market.latest_quotes AS
SELECT
    venue,
    symbol,
    argMaxIf(price, event_time_ms, price IS NOT NULL) AS price,
    argMaxIf(bid_price, event_time_ms, bid_price IS NOT NULL) AS bid,
    argMaxIf(ask_price, event_time_ms, ask_price IS NOT NULL) AS ask,
    max(event_time) AS event_time
FROM market.events_hot
WHERE event_type IN ('trade', 'ticker', 'book_ticker')
GROUP BY venue, symbol;

CREATE TABLE IF NOT EXISTS market.alerts
(
    alert_id String,
    rule_id LowCardinality(String),
    source_topic LowCardinality(String),
    source_id String,
    venue LowCardinality(String),
    symbol LowCardinality(String),
    event_type LowCardinality(String),
    field LowCardinality(String),
    operator LowCardinality(String),
    threshold Float64,
    observed Float64,
    event_time DateTime64(3, 'UTC'),
    event_time_ms Int64,
    created_at DateTime64(3, 'UTC'),
    created_at_ms Int64,
    details_json String
)
ENGINE = ReplacingMergeTree(created_at_ms)
PARTITION BY toYYYYMM(event_time)
ORDER BY (venue, symbol, rule_id, event_time, alert_id)
TTL event_time + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS market.alerts_kafka
(
    alert_id String,
    rule_id String,
    source_topic String,
    source_id String,
    venue String,
    symbol String,
    event_type String,
    field String,
    operator String,
    threshold Float64,
    observed Float64,
    event_time_ms Int64,
    created_at_ms Int64,
    details_json String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'market.alerts',
    kafka_group_name = 'clickhouse-alerts-v4',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS market.alerts_to_hot
TO market.alerts
AS
SELECT
    alert_id, rule_id, source_topic, source_id, venue, symbol, event_type, field, operator,
    threshold, observed,
    fromUnixTimestamp64Milli(event_time_ms) AS event_time,
    event_time_ms,
    fromUnixTimestamp64Milli(created_at_ms) AS created_at,
    created_at_ms,
    details_json
FROM market.alerts_kafka;
