-- 0. EXTENSIONS
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";    -- uuid_generate_v4()
CREATE EXTENSION IF NOT EXISTS "pgcrypto";     -- gen_random_uuid(), cryptographic helpers
CREATE EXTENSION IF NOT EXISTS "pg_trgm";      -- GIN trigram index for fuzzy text search
CREATE EXTENSION IF NOT EXISTS "vector";       -- pgvector: store NLP embeddings (IndoBERT/RoBERTa)
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements"; -- query performance monitoring

CREATE TABLE IF NOT EXISTS app_config (
  key character varying not null,
  value jsonb null,
  created_at timestamp with time zone not null default now(),
  constraint app_config_pkey primary key (key)
) TABLESPACE pg_default;

-- 1. RAW STAGING LAYER
--    Replaces flat JSON files on server.
--    Partitioned quarterly by posted_at so each Airflow daily run only
--    touches one small child table.

--  1a. raw_tweets 
CREATE TABLE IF NOT EXISTS raw_tweets (
    id              TEXT            NOT NULL,
    fullname        TEXT,
    username        TEXT,
    text_content    TEXT,
    embedding       vector(768),                        -- IndoBERT/RoBERTa embedding
    posted_at       TIMESTAMPTZ     NOT NULL,
    like_count      INTEGER         NOT NULL DEFAULT 0,
    comment_count   INTEGER         NOT NULL DEFAULT 0,
    retweet_count   INTEGER         NOT NULL DEFAULT 0,
    quote_count     INTEGER         NOT NULL DEFAULT 0,
    scraped_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    is_processed    BOOLEAN         NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, posted_at)
) PARTITION BY RANGE (posted_at);

-- Quarterly partitions 2023-2026
CREATE TABLE raw_tweets_2023_q1 PARTITION OF raw_tweets FOR VALUES FROM ('2023-01-01') TO ('2023-04-01');
CREATE TABLE raw_tweets_2023_q2 PARTITION OF raw_tweets FOR VALUES FROM ('2023-04-01') TO ('2023-07-01');
CREATE TABLE raw_tweets_2023_q3 PARTITION OF raw_tweets FOR VALUES FROM ('2023-07-01') TO ('2023-10-01');
CREATE TABLE raw_tweets_2023_q4 PARTITION OF raw_tweets FOR VALUES FROM ('2023-10-01') TO ('2024-01-01');
CREATE TABLE raw_tweets_2024_q1 PARTITION OF raw_tweets FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
CREATE TABLE raw_tweets_2024_q2 PARTITION OF raw_tweets FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
CREATE TABLE raw_tweets_2024_q3 PARTITION OF raw_tweets FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');
CREATE TABLE raw_tweets_2024_q4 PARTITION OF raw_tweets FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');
CREATE TABLE raw_tweets_2025_q1 PARTITION OF raw_tweets FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');
CREATE TABLE raw_tweets_2025_q2 PARTITION OF raw_tweets FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');
CREATE TABLE raw_tweets_2025_q3 PARTITION OF raw_tweets FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');
CREATE TABLE raw_tweets_2025_q4 PARTITION OF raw_tweets FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');
CREATE TABLE raw_tweets_2026_q1 PARTITION OF raw_tweets FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');
CREATE TABLE raw_tweets_2026_q2 PARTITION OF raw_tweets FOR VALUES FROM ('2026-04-01') TO ('2026-07-01');
CREATE TABLE raw_tweets_2026_q3 PARTITION OF raw_tweets FOR VALUES FROM ('2026-07-01') TO ('2026-10-01');
CREATE TABLE raw_tweets_2026_q4 PARTITION OF raw_tweets FOR VALUES FROM ('2026-10-01') TO ('2027-01-01');
CREATE TABLE raw_tweets_default  PARTITION OF raw_tweets DEFAULT;  -- safety net

--  1b. raw_reddit 
CREATE TABLE IF NOT EXISTS raw_reddit (
    id              TEXT            NOT NULL,
    username        TEXT,
    title           TEXT,
    body            TEXT,
    -- combined title + body stored pre-normalised for NLP
    text_content    TEXT GENERATED ALWAYS AS (
                        COALESCE(title, '') || ' ' || COALESCE(body, '')
                    ) STORED,
    embedding       vector(768),
    subreddit       TEXT,
    posted_at       TIMESTAMPTZ     NOT NULL,
    score           INTEGER         NOT NULL DEFAULT 0,
    upvote_count    INTEGER         NOT NULL DEFAULT 0,
    downvote_count  INTEGER         NOT NULL DEFAULT 0,
    upvote_ratio    NUMERIC(4, 3),
    comment_count   INTEGER         NOT NULL DEFAULT 0,
    permalink       TEXT,
    scraped_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    is_processed    BOOLEAN         NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, posted_at)
) PARTITION BY RANGE (posted_at);

-- Quarterly partitions 2023-2026
CREATE TABLE raw_reddit_2023_q1 PARTITION OF raw_reddit FOR VALUES FROM ('2023-01-01') TO ('2023-04-01');
CREATE TABLE raw_reddit_2023_q2 PARTITION OF raw_reddit FOR VALUES FROM ('2023-04-01') TO ('2023-07-01');
CREATE TABLE raw_reddit_2023_q3 PARTITION OF raw_reddit FOR VALUES FROM ('2023-07-01') TO ('2023-10-01');
CREATE TABLE raw_reddit_2023_q4 PARTITION OF raw_reddit FOR VALUES FROM ('2023-10-01') TO ('2024-01-01');
CREATE TABLE raw_reddit_2024_q1 PARTITION OF raw_reddit FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
CREATE TABLE raw_reddit_2024_q2 PARTITION OF raw_reddit FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
CREATE TABLE raw_reddit_2024_q3 PARTITION OF raw_reddit FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');
CREATE TABLE raw_reddit_2024_q4 PARTITION OF raw_reddit FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');
CREATE TABLE raw_reddit_2025_q1 PARTITION OF raw_reddit FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');
CREATE TABLE raw_reddit_2025_q2 PARTITION OF raw_reddit FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');
CREATE TABLE raw_reddit_2025_q3 PARTITION OF raw_reddit FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');
CREATE TABLE raw_reddit_2025_q4 PARTITION OF raw_reddit FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');
CREATE TABLE raw_reddit_2026_q1 PARTITION OF raw_reddit FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');
CREATE TABLE raw_reddit_2026_q2 PARTITION OF raw_reddit FOR VALUES FROM ('2026-04-01') TO ('2026-07-01');
CREATE TABLE raw_reddit_2026_q3 PARTITION OF raw_reddit FOR VALUES FROM ('2026-07-01') TO ('2026-10-01');
CREATE TABLE raw_reddit_2026_q4 PARTITION OF raw_reddit FOR VALUES FROM ('2026-10-01') TO ('2027-01-01');
CREATE TABLE raw_reddit_default  PARTITION OF raw_reddit DEFAULT;


-- 2. DIMENSION TABLES

--  2a. dim_time 
-- Pre-populated by a helper function (see section 5).
-- Supports the full hierarchy: Year > Quarter > Month > Week > Day
CREATE TABLE IF NOT EXISTS dim_time (
    time_id         SERIAL          PRIMARY KEY,
    full_date       DATE            NOT NULL UNIQUE,
    year            SMALLINT        NOT NULL,
    quarter         SMALLINT        NOT NULL    CHECK (quarter BETWEEN 1 AND 4),
    month           SMALLINT        NOT NULL    CHECK (month BETWEEN 1 AND 12),
    month_name      TEXT            NOT NULL,
    week_of_year    SMALLINT        NOT NULL,
    day_of_month    SMALLINT        NOT NULL,
    day_of_week     SMALLINT        NOT NULL    CHECK (day_of_week BETWEEN 1 AND 7),
    day_name        TEXT            NOT NULL,
    is_weekend      BOOLEAN         NOT NULL
                    GENERATED ALWAYS AS (day_of_week IN (6, 7)) STORED
);

--  2b. dim_platform 
-- Hierarchy: Platform > Channel/Subreddit
CREATE TABLE IF NOT EXISTS dim_platform (
    platform_id     SERIAL          PRIMARY KEY,
    platform_name   TEXT            NOT NULL,   -- 'X' | 'Reddit'
    channel         TEXT            NOT NULL,   -- subreddit or 'X_global'
    UNIQUE (platform_name, channel)
);

INSERT INTO dim_platform (platform_name, channel) VALUES
    ('X',      'X_global'),
    ('Reddit', 'r/indonesia'),
    ('Reddit', 'r/economy'),
    ('Reddit', 'r/investasi'),
    ('Reddit', 'r/personalfinance')
ON CONFLICT DO NOTHING;

--  2c. dim_topic ─
-- Populated by LDA topic modeling during the Transform stage.
CREATE TABLE IF NOT EXISTS dim_topic (
    topic_id        SERIAL          PRIMARY KEY,
    topic_label     TEXT            NOT NULL UNIQUE,   -- 'inflasi', 'BBM', 'UMR', ...
    topic_category  TEXT,                              -- broader group e.g. 'harga', 'tenaga_kerja'
    top_keywords    TEXT[]                             -- top-N LDA terms
);

--  2d. dim_sentiment ─
-- Each row is a unique (label × confidence_bucket) combination.
-- confidence_bucket is derived from the model's score:
--   High ≥ 0.80 | Medium 0.50–0.79 | Low < 0.50
CREATE TABLE IF NOT EXISTS dim_sentiment (
    sentiment_id        SERIAL      PRIMARY KEY,
    sentiment_label     TEXT        NOT NULL,   -- 'positive' | 'negative' | 'neutral'
    confidence_bucket   TEXT        NOT NULL    CHECK (confidence_bucket IN ('High', 'Medium', 'Low')),
    UNIQUE (sentiment_label, confidence_bucket)
);

INSERT INTO dim_sentiment (sentiment_label, confidence_bucket) VALUES
    ('positive', 'High'),   ('positive', 'Medium'),   ('positive', 'Low'),
    ('negative', 'High'),   ('negative', 'Medium'),   ('negative', 'Low'),
    ('neutral',  'High'),   ('neutral',  'Medium'),   ('neutral',  'Low')
ON CONFLICT DO NOTHING;


-- 3. FACT TABLE  (partitioned by year)

CREATE TABLE IF NOT EXISTS fact_post (
    post_id         BIGSERIAL,
    source_id       TEXT            NOT NULL,   -- original tweet/reddit ID
    source_url      TEXT,
    -- foreign keys
    time_id         INTEGER         NOT NULL    REFERENCES dim_time(time_id),
    platform_id     INTEGER         NOT NULL    REFERENCES dim_platform(platform_id),
    topic_id        INTEGER                     REFERENCES dim_topic(topic_id),
    sentiment_id    INTEGER                     REFERENCES dim_sentiment(sentiment_id),
    -- measures
    like_count      INTEGER         NOT NULL DEFAULT 0,
    comment_count   INTEGER         NOT NULL DEFAULT 0,
    quote_count     INTEGER         NOT NULL DEFAULT 0,
    retweet_count   INTEGER         NOT NULL DEFAULT 0,
    upvote_count    INTEGER         NOT NULL DEFAULT 0,
    downvote_count  INTEGER         NOT NULL DEFAULT 0,
    sentiment_score NUMERIC(6, 5),              -- raw model probability 0.0-1.0
    -- engagement_tier derived from power-law distribution (Low / Medium / High / Viral)
    engagement_tier TEXT            GENERATED ALWAYS AS (
        CASE
            WHEN (like_count + retweet_count + upvote_count + comment_count) = 0 THEN 'Zero'
            WHEN (like_count + retweet_count + upvote_count + comment_count) < 10   THEN 'Low'
            WHEN (like_count + retweet_count + upvote_count + comment_count) < 100  THEN 'Medium'
            WHEN (like_count + retweet_count + upvote_count + comment_count) < 1000 THEN 'High'
            ELSE 'Viral'
        END
    ) STORED,
    posted_at       TIMESTAMPTZ     NOT NULL,   -- kept in fact for partition pruning
    loaded_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (post_id, posted_at)
) PARTITION BY RANGE (posted_at);

CREATE TABLE fact_post_2023 PARTITION OF fact_post FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE fact_post_2024 PARTITION OF fact_post FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE fact_post_2025 PARTITION OF fact_post FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE fact_post_2026 PARTITION OF fact_post FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE fact_post_default PARTITION OF fact_post DEFAULT;


-- 4. INDEXES

-- raw_tweets: ETL processing queue + trigram full-text search
CREATE INDEX idx_raw_tweets_unprocessed   ON raw_tweets (scraped_at)       WHERE is_processed = FALSE;
CREATE INDEX idx_raw_tweets_text_trgm     ON raw_tweets USING GIN (text_content gin_trgm_ops);
CREATE INDEX idx_raw_tweets_username      ON raw_tweets (username);

-- raw_reddit: same pattern
CREATE INDEX idx_raw_reddit_unprocessed   ON raw_reddit (scraped_at)       WHERE is_processed = FALSE;
CREATE INDEX idx_raw_reddit_text_trgm     ON raw_reddit USING GIN (text_content gin_trgm_ops);
CREATE INDEX idx_raw_reddit_subreddit     ON raw_reddit (subreddit);

-- dim_time: OLAP drill-down paths
CREATE INDEX idx_dim_time_year            ON dim_time (year);
CREATE INDEX idx_dim_time_year_quarter    ON dim_time (year, quarter);
CREATE INDEX idx_dim_time_year_month      ON dim_time (year, month);

-- fact_post: all FK columns for OLAP joins + slice/dice
CREATE INDEX idx_fact_post_time           ON fact_post (time_id);
CREATE INDEX idx_fact_post_platform       ON fact_post (platform_id);
CREATE INDEX idx_fact_post_topic          ON fact_post (topic_id);
CREATE INDEX idx_fact_post_sentiment      ON fact_post (sentiment_id);
CREATE INDEX idx_fact_post_tier           ON fact_post (engagement_tier);
-- compound for the four main OLAP queries
CREATE INDEX idx_fact_post_olap_temporal  ON fact_post (time_id, topic_id, sentiment_id);
CREATE INDEX idx_fact_post_olap_platform  ON fact_post (platform_id, topic_id, time_id);

-- pgvector cosine-similarity search on embeddings
CREATE INDEX idx_raw_tweets_embedding     ON raw_tweets USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
CREATE INDEX idx_raw_reddit_embedding     ON raw_reddit USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);


-- 5. HELPER FUNCTIONS

-- 5a. Populate dim_time for a date range (call once at init)
CREATE OR REPLACE FUNCTION populate_dim_time(start_date DATE, end_date DATE)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    d DATE := start_date;
BEGIN
    WHILE d <= end_date LOOP
        INSERT INTO dim_time (
            full_date, year, quarter, month, month_name,
            week_of_year, day_of_month, day_of_week, day_name
        )
        VALUES (
            d,
            EXTRACT(YEAR    FROM d)::SMALLINT,
            EXTRACT(QUARTER FROM d)::SMALLINT,
            EXTRACT(MONTH   FROM d)::SMALLINT,
            TO_CHAR(d, 'Month'),
            EXTRACT(WEEK    FROM d)::SMALLINT,
            EXTRACT(DAY     FROM d)::SMALLINT,
            EXTRACT(ISODOW  FROM d)::SMALLINT,
            TO_CHAR(d, 'Day')
        )
        ON CONFLICT (full_date) DO NOTHING;
        d := d + 1;
    END LOOP;
END;
$$;

-- Seed the full project range (call once after applying migrations)
-- SELECT populate_dim_time('2023-01-01', '2026-12-31');

-- 5b. Resolve sentiment_id from model outputs
CREATE OR REPLACE FUNCTION get_sentiment_id(
    p_label TEXT,
    p_score NUMERIC
) RETURNS INTEGER LANGUAGE sql STABLE AS $$
    SELECT sentiment_id FROM dim_sentiment
    WHERE sentiment_label = p_label
      AND confidence_bucket = CASE
            WHEN p_score >= 0.80 THEN 'High'
            WHEN p_score >= 0.50 THEN 'Medium'
            ELSE 'Low'
          END
    LIMIT 1;
$$;


-- 6. MATERIALIZED VIEWS  (refreshed by Airflow after each load)
--    Each view has a UNIQUE index so REFRESH CONCURRENTLY is possible,
--    meaning Airflow refreshes do not block live reads.

--  6a. Temporal Sentiment Trends 
-- Insight 1: How average sentiment on a topic shifts month-to-month.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_temporal_sentiment AS
SELECT
    dt.year,
    dt.quarter,
    dt.month,
    dt.month_name,
    dtp.topic_label,
    ds.sentiment_label,
    dp.platform_name,
    COUNT(*)                        AS post_count,
    AVG(fp.sentiment_score)         AS avg_sentiment_score,
    STDDEV(fp.sentiment_score)      AS stddev_sentiment_score
FROM fact_post       fp
JOIN dim_time        dt  ON fp.time_id      = dt.time_id
JOIN dim_topic       dtp ON fp.topic_id     = dtp.topic_id
JOIN dim_sentiment   ds  ON fp.sentiment_id = ds.sentiment_id
JOIN dim_platform    dp  ON fp.platform_id  = dp.platform_id
GROUP BY
    dt.year, dt.quarter, dt.month, dt.month_name,
    dtp.topic_label, ds.sentiment_label, dp.platform_name
WITH DATA;

CREATE UNIQUE INDEX uidx_mv_temporal_sentiment
    ON mv_temporal_sentiment (year, month, topic_label, sentiment_label, platform_name);

--  6b. Cross-Platform Sentiment Comparison ─
-- Insight 2: X vs Reddit sentiment for the same topic + time window.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_platform_comparison AS
SELECT
    dp.platform_name,
    dp.channel,
    dt.year,
    dt.quarter,
    dtp.topic_label,
    ds.sentiment_label,
    ds.confidence_bucket,
    COUNT(*)                        AS post_count,
    AVG(fp.sentiment_score)         AS avg_sentiment_score,
    -- engagement as a single cross-platform metric
    AVG(fp.like_count + fp.retweet_count + fp.upvote_count + fp.comment_count)
                                    AS avg_engagement
FROM fact_post       fp
JOIN dim_platform    dp  ON fp.platform_id  = dp.platform_id
JOIN dim_time        dt  ON fp.time_id      = dt.time_id
JOIN dim_topic       dtp ON fp.topic_id     = dtp.topic_id
JOIN dim_sentiment   ds  ON fp.sentiment_id = ds.sentiment_id
GROUP BY
    dp.platform_name, dp.channel,
    dt.year, dt.quarter,
    dtp.topic_label, ds.sentiment_label, ds.confidence_bucket
WITH DATA;

CREATE UNIQUE INDEX uidx_mv_platform_comparison
    ON mv_platform_comparison (platform_name, channel, year, quarter, topic_label, sentiment_label, confidence_bucket);

--  6c. Topic Volume Distribution ─
-- Insight 3: Most-discussed topics per quarter; sustained vs event-driven.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_topic_volume AS
SELECT
    dt.year,
    dt.quarter,
    dtp.topic_label,
    dtp.topic_category,
    dp.platform_name,
    COUNT(*)                        AS post_count,
    RANK() OVER (
        PARTITION BY dt.year, dt.quarter, dp.platform_name
        ORDER BY COUNT(*) DESC
    )                               AS topic_rank
FROM fact_post       fp
JOIN dim_time        dt  ON fp.time_id   = dt.time_id
JOIN dim_topic       dtp ON fp.topic_id  = dtp.topic_id
JOIN dim_platform    dp  ON fp.platform_id = dp.platform_id
GROUP BY
    dt.year, dt.quarter,
    dtp.topic_label, dtp.topic_category,
    dp.platform_name
WITH DATA;

CREATE UNIQUE INDEX uidx_mv_topic_volume
    ON mv_topic_volume (year, quarter, topic_label, platform_name);

--  6d. Engagement × Sentiment Correlation 
-- Insight 4: Do negative posts attract more interaction than neutral/positive?
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_engagement_by_sentiment AS
SELECT
    ds.sentiment_label,
    ds.confidence_bucket,
    dp.platform_name,
    fp.engagement_tier,
    dt.year,
    dt.quarter,
    COUNT(*)                                        AS post_count,
    AVG(fp.like_count)                              AS avg_likes,
    AVG(fp.retweet_count)                           AS avg_retweets,
    AVG(fp.comment_count)                           AS avg_comments,
    AVG(fp.upvote_count)                            AS avg_upvotes,
    AVG(fp.upvote_count - fp.downvote_count)        AS avg_net_votes,
    AVG(fp.like_count + fp.retweet_count + fp.upvote_count + fp.comment_count)
                                                    AS avg_total_engagement
FROM fact_post       fp
JOIN dim_sentiment   ds  ON fp.sentiment_id = ds.sentiment_id
JOIN dim_platform    dp  ON fp.platform_id  = dp.platform_id
JOIN dim_time        dt  ON fp.time_id      = dt.time_id
GROUP BY
    ds.sentiment_label, ds.confidence_bucket,
    dp.platform_name, fp.engagement_tier,
    dt.year, dt.quarter
WITH DATA;

CREATE UNIQUE INDEX uidx_mv_engagement_by_sentiment
    ON mv_engagement_by_sentiment (sentiment_label, confidence_bucket, platform_name, engagement_tier, year, quarter);


-- 
-- 7. RPC FUNCTIONS  (called via Supabase REST or Airflow)
-- 

-- 7a. Refresh all materialized views concurrently (non-blocking)
CREATE OR REPLACE FUNCTION refresh_all_views()
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_temporal_sentiment;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_platform_comparison;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_topic_volume;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_engagement_by_sentiment;
END;
$$;

-- 7b. Return unprocessed raw rows for the Transform stage
--     Called by Airflow transform task for each platform.
CREATE OR REPLACE FUNCTION get_unprocessed_raw(
    p_platform  TEXT,           -- 'tweets' | 'reddit'
    p_limit     INTEGER DEFAULT 1000
)
RETURNS TABLE (
    id          TEXT,
    text_content TEXT,
    posted_at   TIMESTAMPTZ,
    extra       JSONB
) LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    IF p_platform = 'tweets' THEN
        RETURN QUERY
        SELECT
            rt.id,
            rt.text_content,
            rt.posted_at,
            jsonb_build_object(
                'username',      rt.username,
                'like_count',    rt.like_count,
                'retweet_count', rt.retweet_count,
                'comment_count', rt.comment_count,
                'quote_count',   rt.quote_count
            )
        FROM raw_tweets rt
        WHERE rt.is_processed = FALSE
        ORDER BY rt.scraped_at
        LIMIT p_limit;
    ELSE
        RETURN QUERY
        SELECT
            rr.id,
            rr.text_content,
            rr.posted_at,
            jsonb_build_object(
                'username',      rr.username,
                'subreddit',     rr.subreddit,
                'score',         rr.score,
                'upvote_ratio',  rr.upvote_ratio,
                'comment_count', rr.comment_count,
                'permalink',     rr.permalink
            )
        FROM raw_reddit rr
        WHERE rr.is_processed = FALSE
        ORDER BY rr.scraped_at
        LIMIT p_limit;
    END IF;
END;
$$;

-- 7c. Mark rows as processed after Transform stage finishes
CREATE OR REPLACE FUNCTION mark_as_processed(
    p_platform TEXT,
    p_ids      TEXT[]
)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    IF p_platform = 'tweets' THEN
        UPDATE raw_tweets SET is_processed = TRUE WHERE id = ANY(p_ids);
    ELSE
        UPDATE raw_reddit SET is_processed = TRUE WHERE id = ANY(p_ids);
    END IF;
END;
$$;


-- 
-- 8. ROW-LEVEL SECURITY
--    Service-role key (used by Airflow) bypasses RLS.
--    Anon / authenticated users get read-only access to DWH tables.
-- 

ALTER TABLE raw_tweets      ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_reddit      ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_time        ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_platform    ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_topic       ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_sentiment   ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_post       ENABLE ROW LEVEL SECURITY;

-- Raw tables: service role only (Airflow pipeline)
CREATE POLICY raw_tweets_service_only ON raw_tweets
    USING (auth.role() = 'service_role');

CREATE POLICY raw_reddit_service_only ON raw_reddit
    USING (auth.role() = 'service_role');

-- DWH / dimension tables: public read (for Atoti OLAP)
CREATE POLICY dim_time_read_all        ON dim_time        FOR SELECT USING (TRUE);
CREATE POLICY dim_platform_read_all    ON dim_platform    FOR SELECT USING (TRUE);
CREATE POLICY dim_topic_read_all       ON dim_topic       FOR SELECT USING (TRUE);
CREATE POLICY dim_sentiment_read_all   ON dim_sentiment   FOR SELECT USING (TRUE);
CREATE POLICY fact_post_read_all       ON fact_post       FOR SELECT USING (TRUE);

-- Writes to DWH tables are service-role only
CREATE POLICY dim_time_service_write      ON dim_time        FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY dim_platform_service_write  ON dim_platform    FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY dim_topic_service_write     ON dim_topic       FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY dim_sentiment_service_write ON dim_sentiment   FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY fact_post_service_write     ON fact_post       FOR ALL USING (auth.role() = 'service_role');

ALTER TABLE raw_tweets ADD COLUMN IF NOT EXISTS clean_text TEXT;
ALTER TABLE raw_reddit ADD COLUMN IF NOT EXISTS clean_text TEXT;

ALTER TABLE raw_tweets DROP COLUMN IF EXISTS embedding;
ALTER TABLE raw_reddit DROP COLUMN IF EXISTS embedding;