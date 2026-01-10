from sqlalchemy import text

DATA_QUERY = text("""
SELECT
    m.series_id,
    m.dataset_id,
    m.description,
    m.unit,
    m.frequency,
    d.observation_time,
    d.value,
    d.quality_flag,
    d.raw_payload
FROM meta_series m
JOIN data_observations d
  ON m.series_id = d.series_id
WHERE (:series_id IS NULL OR m.series_id = :series_id)
  AND (:dataset_id IS NULL OR m.dataset_id = :dataset_id)
  AND (:start IS NULL OR d.observation_time >= :start)
  AND (:end IS NULL OR d.observation_time <= :end)
  AND (:quality_flag IS NULL OR d.quality_flag = :quality_flag)
  AND (:min_value IS NULL OR d.value >= :min_value)
  AND (:max_value IS NULL OR d.value <= :max_value)
ORDER BY d.observation_time
LIMIT :limit OFFSET :offset
""")
