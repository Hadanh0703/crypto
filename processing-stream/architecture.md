# 🚀 Hệ Thống Big Data Phân Tích Crypto (Crypto Intelligence System)

---

## 1. 📌 Tổng quan

Dự án xây dựng một hệ thống **Big Data pipeline** để thu thập, xử lý, phân tích và trực quan hóa dữ liệu tiền mã hóa theo thời gian thực.

Hệ thống tích hợp:

* Dữ liệu thị trường (OHLCV, giao dịch)
* Tin tức crypto và phân tích cảm xúc (sentiment)

👉 Mục tiêu:

* Phát hiện tín hiệu giao dịch (pump/dump)
* Phân tích ảnh hưởng của tin tức đến giá
* Mô phỏng chiến lược giao dịch (winrate)

---

## 2. 🧠 Bài toán

Thị trường crypto tạo ra:

* Dữ liệu giá với tần suất cao (high-frequency)
* Dữ liệu tin tức phi cấu trúc

Các đặc điểm:

* Khối lượng lớn (volume)
* Tốc độ cao (velocity)
* Dữ liệu bán cấu trúc (semi-structured)

👉 Hệ thống truyền thống khó xử lý hiệu quả theo thời gian thực.

---

## 3. 🏗️ Kiến trúc hệ thống

```text id="z64bfk"
Data Sources
   ↓
Kafka (Streaming)
   ↓
Spark Streaming
   ↓
MinIO (raw data lake)
   ↓
CronJob Batch Backfill
   ↓
Spark Batch Processing
   ↓
--------------------------
|                        |
ClickHouse             MongoDB
(analytics)            (news + sentiment)
   ↓                        ↓
        API Layer (NestJS)
                ↓
        Dashboard / Frontend
```

---

## 4. ⚙️ Công nghệ sử dụng

### 🔹 Thu thập dữ liệu (Ingestion)

* Kafka – pipeline streaming
* Binance API – dữ liệu giá
* API / crawler tin tức

---

### 🔹 Xử lý dữ liệu

* Spark (batch + streaming)
* Spark SQL
* NLP (phân tích sentiment)

---

### 🔹 Lưu trữ

#### Data Lake

* MinIO – lưu dữ liệu thô theo chuẩn S3-compatible object storage

#### Database phân tích

* ClickHouse – truy vấn nhanh OHLCV và metrics

#### Database tài liệu

* MongoDB – lưu tin tức và sentiment

---

### 🔹 Backend

* NestJS (API layer)

---

### 🔹 Trực quan hóa

* Grafana (dashboard realtime)
* React + Tailwind (UI tùy chỉnh)

---

## 5. 📊 Nguồn dữ liệu

### 🔥 5.1 Dữ liệu giá (OHLCV)

Nguồn chính:

* Binance API

Bao gồm:

* OHLCV (candlestick)
* giao dịch
* order book

Đặc điểm:

* realtime qua WebSocket
* dữ liệu lịch sử qua REST API

---

### 📰 5.2 Dữ liệu tin tức

Nguồn chính:

* CoinDesk

Cung cấp:

* tin tức crypto
* phân tích thị trường

---

### 🔥 5.3 Các nguồn tin khác (khuyến nghị)

Để tăng độ phủ dữ liệu:

#### 🟢 Website crypto:

* CoinTelegraph
* Decrypt
* CryptoPanic

#### 🟢 API tổng hợp:

* NewsAPI
* GDELT
* ChainGPT News API

👉 Giúp:

* tổng hợp nhiều nguồn
* giảm trùng lặp
* enrich dữ liệu

---

### 🟢 Dữ liệu mạng xã hội (nâng cao)

* Twitter/X
* Reddit
* Telegram

👉 Dùng để:

* phát hiện tín hiệu sớm
* phân tích sentiment

---

## 6. 🔄 Pipeline dữ liệu

### 🔹 Luồng realtime

```text id="qn40pb"
Binance / News → Kafka → Spark Streaming → MinIO (raw)
```

* lưu dữ liệu thô theo thời gian thực:

  * raw price events
  * raw news events
  * dữ liệu gốc phục vụ replay/backfill

---

### 🔹 Luồng batch

```text id="qmx8ci"
CronJob → MinIO → Spark Batch Processing → ClickHouse / MongoDB
```

* tính toán:

  * moving average
  * volatility
  * correlation
  * sentiment score
  * dữ liệu enrich cho dashboard và API

---

## 7. 🧪 Xử lý dữ liệu

### 🔹 Feature từ giá

* Moving Average (MA)
* RSI
* phát hiện volume spike

---

### 🔹 Xử lý tin tức

* phân tích sentiment (NLP)
* trích xuất keyword
* nhận diện entity (BTC, ETH…)

---

### 🔹 Phát hiện tín hiệu

* pump/dump
* ảnh hưởng tin tức đến giá

---

## 8. 📈 Output & Visualization

Dashboard bao gồm:

### 📊 Dữ liệu thị trường

* biểu đồ OHLCV
* chỉ báo (MA, RSI)

---

### 📰 Tin tức

* news realtime
* sentiment score

---

### 🚀 Tín hiệu

* tín hiệu mua/bán
* phát hiện bất thường

---

### 📊 Phân tích

* winrate
* đánh giá chiến lược

---

## 9. 🎯 Tính năng chính

* Xử lý dữ liệu realtime
* Kết hợp nhiều nguồn dữ liệu (price + news)
* Phát hiện tín hiệu giao dịch
* Kiến trúc Big Data có thể scale
* Dashboard trực quan

---

## 10. 🚀 Hướng phát triển

* Machine Learning dự đoán giá
* Trading agent (AI)
* Multi-exchange aggregation
* NLP nâng cao (LLM)

---

## 11. 🚢 Triển khai với Kubernetes

Hệ thống được triển khai trên Kubernetes nhằm đảm bảo khả năng mở rộng và ổn định.

---

### 11.1 🎯 Mục tiêu

* Auto scaling
* High availability
* Dễ deploy và update
* Quản lý container tập trung

---

### 11.2 🧱 Thành phần Kubernetes

#### Namespace

* dev / staging / production

---

#### Deployment (stateless)

* API (NestJS)
* Spark jobs
* Kafka consumers

---

#### StatefulSet (stateful)

* Kafka
* ClickHouse
* MongoDB
* MinIO

---

#### Service

* ClusterIP (internal)
* LoadBalancer (external)

---

#### Ingress

* expose domain:

  * api.domain.com
  * grafana.domain.com

---

#### ConfigMap & Secret

* config app
* API key
* DB password

---

#### Persistent Volume (PV) & PVC

* lưu dữ liệu cho:

  * Kafka
  * ClickHouse
  * MongoDB

---

#### HPA (Horizontal Pod Autoscaler)

* scale theo CPU / memory

---

#### CronJob

* chạy batch backfill:

  * đọc raw data từ MinIO
  * chạy Spark batch processing
  * ghi dữ liệu đã xử lý sang ClickHouse và MongoDB
  * data cleanup

---

### 11.3 🏗️ Kiến trúc deploy

```text id="m3u9pm"
Ingress
   ↓
API (NestJS)
   ↓
---------------------
|                   |
ClickHouse        MongoDB
   ↑                ↑
   |                |
Spark Batch Processing
   ↑
CronJob Backfill
   ↑
MinIO (raw)
   ↑
Spark Streaming
   ↑
Kafka
```

---

### 11.4 📦 Mapping service

| Component  | K8s Resource     |
| ---------- | ---------------- |
| Kafka      | StatefulSet      |
| Spark Streaming | Deployment |
| Spark Batch | Job / CronJob |
| MinIO      | StatefulSet      |
| ClickHouse | StatefulSet      |
| MongoDB    | StatefulSet      |
| API        | Deployment       |
| Grafana    | Deployment       |

---

### 11.5 🔄 CI/CD (khuyến nghị)

* Docker build
* Push registry
* Deploy:

  * Helm / ArgoCD / GitHub Actions

---

### 11.6 📈 Monitoring

* Grafana + Prometheus
* theo dõi:

  * CPU / memory
  * Kafka lag
  * Spark job

---

## 12. 📌 Kết luận

Hệ thống thể hiện đầy đủ pipeline Big Data:

* ingestion
* storage
* processing
* analytics
* visualization

👉 Biến dữ liệu crypto thô thành insight có giá trị.

---

## 13. 🎯 Tổng kết

```text id="5zy23t"
Kafka → Spark → MinIO (raw)
                 ↓
        CronJob Backfill
                 ↓
          Spark Batch Processing
                 ↓
        ClickHouse + MongoDB
                 ↓
           API → Frontend
```

👉 Một hệ thống Big Data hoàn chỉnh từ ingestion → insight.

---
