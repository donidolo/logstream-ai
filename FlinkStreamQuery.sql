-- Query 1: Windowed error-rate aggregation with anomaly detection
SELECT
  service_name,
  window_start AS window_time,
  error_count,
  ML_DETECT_ANOMALIES(
    error_count,
    window_start,
    JSON_OBJECT('horizon' VALUE 1, 'confidencePercentage' VALUE 90.0)
  ) OVER (
    PARTITION BY service_name
    ORDER BY window_start
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS anomaly_result
FROM (
  SELECT
    window_start,
    window_end,
    service_name,
    COUNT(*) FILTER (WHERE log_level IN ('ERROR', 'CRITICAL')) AS error_count
  FROM TABLE(
    TUMBLE(TABLE `raw-logs`, DESCRIPTOR(`$rowtime`), INTERVAL '1' MINUTE)
  )
  GROUP BY window_start, window_end, service_name
);

-- Query 2: AI-powered diagnosis via Claude Haiku (INSERT INTO sink topic)
INSERT INTO `ai_alerts_stream`
SELECT
  service_name,
  log_level,
  message,
  response_time_ms,
  diagnosis,
  `$rowtime` AS alert_time
FROM `raw-logs`
CROSS JOIN LATERAL TABLE(
  AI_COMPLETE(
    'log_diagnosis_model',
    CONCAT(
      'You are a DevOps SRE assistant. A critical log was flagged. ',
      'Service: ', service_name,
      ', Level: ', log_level,
      ', Message: ', message,
      ', Response time: ', CAST(response_time_ms AS STRING), 'ms. ',
      'In 2 sentences: give the likely root cause and one recommended action.'
    )
  )
)
WHERE log_level = 'CRITICAL';

-- Supporting: AI model definition
CREATE MODEL `log_diagnosis_model`
INPUT (prompt STRING)
OUTPUT (diagnosis STRING)
WITH (
  'provider' = 'anthropic',
  'anthropic.connection' = 'anthropic-connection',
  'anthropic.params.max_tokens' = '1024',
  'anthropic.model_version' = 'claude-haiku-4-5-20241022',
  'task' = 'text_generation'
);