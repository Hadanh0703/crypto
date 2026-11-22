# Architecture Review

| Severity | Finding/evidence | Impact | Recommendation | Complexity/Priority |
|---|---|---|---|---|
| Critical | Batch MinIO failure falls back Spot then static mock (`kline_batch_job.py:78-106`) | dữ liệu giả/sai Futures vào analytics | fail closed; demo source chỉ qua explicit flag | Low/P0 |
| High | Batch raw path khác stream layout (`kline_batch_job.py:79`; stream `:113-115`) | batch có thể luôn fallback | derive path from env; integration test | Low/P0 |
| High | Collector lấy bucket phút hiện tại, không xác nhận candle đóng | dữ liệu incomplete rồi checkpoint qua | set `toTime=boundary-1`, validate closeTime | Low/P0 |
| High | Redis lock release không owner token (`redis.utils.ts:10-20`) | concurrent duplicate ingestion | random token + Lua compare-delete | Medium/P0 |
| High | Signal rerun insert UUID, thiếu deterministic idempotency | duplicate/unstable signal | key `(ticker,timestamp,type)`, versioned upsert | Medium/P1 |
| High | No auth; ClickHouse/MinIO ingress exposed | unauthorized access | network policy, auth, Secrets, remove public ingress | Medium/P0 |
| Medium | Two ingestion paths with different timeframe/semantics | inconsistent tables/ownership | choose canonical pipeline and disable other | Medium/P1 |
| Medium | sample stddev batch vs population stddev API | indicator mismatch | specify one definition and test fixtures | Low/P1 |
| Medium | `failOnDataLoss=false` | silent Kafka gaps | alert gaps; deliberate policy/DLQ | Medium/P1 |
| Medium | single Kafka/Redis/ClickHouse replicas | SPOF | HA/replication/backups | High/P2 |
| Medium | ClickHouse `latest`, missing resource limits/config objects | unreproducible deployment | pin images; GitOps config templates | Low/P1 |
| Low | no alert rules/dashboards | slow incident detection | service SLO + lag/data freshness alerts | Medium/P2 |

Strengths: raw payload retention, explicit Spark checkpoints, Decimal storage, parameterized ClickHouse queries, FINAL reads, modular backend repositories, frontend error states. Overall: correctness 5/10, reliability 4/10, scalability 6/10, maintainability 6/10, data quality 4/10, performance 6/10, security 3/10, observability 5/10, fault tolerance 3/10, deployment readiness 4/10 (`INFERRED` assessment from static evidence).
