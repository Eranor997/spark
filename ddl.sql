CREATE TABLE default.shkDefect_verdict_local
(
    `shk_id` Int64,
    `place_code` UInt32,
    `reason_id` UInt32,
    `employee_id` UInt32,
    `office_id` UInt32
)
ENGINE = MergeTree
ORDER BY (shk_id)
SETTINGS index_granularity = 8192;
