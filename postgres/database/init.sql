-- Create the dim_metrics table first
CREATE TABLE IF NOT EXISTS dim_metrics (
    id INTEGER PRIMARY KEY,
    name VARCHAR(16)
);

-- Insert values into dim_metrics
INSERT INTO dim_metrics (id, name) VALUES
    (1, 'count'),
    (2, 'min'),
    (3, 'max'),
    (4, 'sum'),
    (5, 'average'),
    (6, 'median'),
    (7, 'stddev');

-- Create the prices table
CREATE TABLE IF NOT EXISTS prices (
    timestamp TIMESTAMP NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    user_id INTEGER
);

CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON prices (timestamp);

-- Create the price_metrics table with the correct foreign key constraint
CREATE TABLE IF NOT EXISTS price_metrics (
    timestamp TIMESTAMP NOT NULL,
    metric_value DOUBLE PRECISION,
    metric_type INTEGER,
    CONSTRAINT FK_dim_metrics FOREIGN KEY (metric_type) REFERENCES dim_metrics(id)
);

CREATE INDEX IF NOT EXISTS idx_price_metrics_timestamp ON price_metrics (timestamp);
CREATE INDEX IF NOT EXISTS idx_price_metrics_metric_type ON price_metrics (metric_type);
