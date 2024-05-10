CREATE TABLE IF NOT EXISTS prices (
    timestamp TIMESTAMP NOT NULL,
    price DOUBLE NOT NULL,
    user_id INTEGER
);

CREATE INDEX idx_prices_timestamp ON prices (timestamp);



CREATE TABLE IF NOT EXISTS price_metrics (
    timestamp TIMESTAMP NOT NULL,
    metric_value DOUBLE,
    metric_type VARCHAR(16),
);

CREATE INDEX idx_price_metrics_timestamp ON price_metrics (timestamp);
CREATE INDEX idx_price_metrics_metric_type ON price_metrics (metric_type);