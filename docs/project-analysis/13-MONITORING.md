# Monitoring & Observability

Manifests cấu hình Prometheus 2.53.1, Grafana 11.1.4, Loki/Promtail 3.1.1, node exporter, kube-state-metrics, Kafka exporter, Redis exporter và blackbox exporter (`CONFIGURED_NOT_EXECUTED`). Prometheus scrape 15s; targets gồm exporters, annotated pods, ClickHouse metrics và blackbox MinIO health. Grafana provision Prometheus/Loki datasources.

Không thấy dashboard JSON, alert rules/Alertmanager, SLO, distributed tracing, application custom metrics hay Kafka consumer lag alert (`NOT_IMPLEMENTED`). Collector/API dùng logs; Spark log progress microbatch. Promtail thu pod logs. Backend `/api` và collector `/api/health-check` là shallow health, không kiểm tra dependency. Redis exporter không cấu hình auth trong manifest; có thể fail nếu Redis yêu cầu password.
