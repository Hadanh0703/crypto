# Repository Map

```text
Crypto/
├─ ingestion-collector/   NestJS cron collector, Redis checkpoint/lock, Kafka producer
├─ processing-stream/     PySpark Structured Streaming: Kafka → MinIO + ClickHouse
├─ processing-batch/      PySpark batch: 1m → 5m, indicators, pump/dump
├─ backend-api/           NestJS REST API, chatbot và worker direct-ingestion riêng
├─ frontend-dashboard/    React/Vite SPA
└─ infra-k8s/             Kubernetes: app, kafka, spark, monitor
```

## Technology stack

| Component | Technology/version | Purpose | Evidence |
|---|---|---|---|
| Collector | Node 20 image; NestJS 10.4; KafkaJS 2.2.4; ioredis 5.4 | Cron ingestion | `ingestion-collector/package.json:20-58`, `Dockerfile:2,19` |
| Streaming | Python; PySpark 3.5.1 | Kafka streaming sinks | `processing-stream/requirements.txt:1` |
| Batch | Python 3.11; PySpark 3.4.2; clickhouse-connect 0.6.5 | aggregation/analytics | `processing-batch/Dockerfile:1`, `requirements.txt:1-2` |
| API | NestJS 11; ClickHouse client 1.19; OpenAI 6.42 | REST/LLM | `backend-api/package.json` |
| UI | React 19.1; Vite 6.3.5; lightweight-charts 4.2.3 | dashboard | `frontend-dashboard/package.json` |
| Infra | Kafka/ZK 7.6.1, Redis 6.2, pinned MinIO; ClickHouse `latest` | runtime | `infra-k8s/**` |

## Entry points

- Collector HTTP: `src/main.ts:45-90`, `yarn start`; worker cron: `src/worker.ts:8-33`, `yarn start:worker`/`dev:worker`.
- Stream: `kafka_raw_to_minio.py:224-251`, submitted by SparkApplication.
- Batch: `main.py:4-18`, scheduled every 5 minutes by `infra-k8s/spark/005-processing-batch.yaml:2-15`.
- API: `backend-api/src/main.ts:10-38`; optional direct worker: `src/worker.ts:7-19`.
- UI: `frontend-dashboard/src/main.tsx`, routes in `src/app/App.tsx:11-21`.

Infrastructure must precede producers/consumers in dependency order: Redis/Kafka/MinIO/ClickHouse → collector/stream/batch/API → frontend. Kubernetes lacks explicit readiness-based ordering (`CONFIGURED_NOT_EXECUTED`).
