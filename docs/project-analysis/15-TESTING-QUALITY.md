# Testing & Code Quality

Backend có một scaffold unit test cho AppController và e2e test mặc định; chưa thấy test nghiệp vụ cho query, news impact, chatbot hoặc workers. Collector package khai báo test commands nhưng các file test/config tương ứng không có trong inventory. Frontend không có test script. Stream/batch không có tests. Không có coverage artifact đáng tin cậy.

Static checks khai báo: TypeScript build/eslint cho Node projects; frontend `tsc -b && vite build`; Python không có formatter/type-check config. Chưa chạy dependency-installing hoặc external-service tests. Do các project không có `node_modules`/venv đảm bảo và test có thể cần service, trạng thái là `NOT_EXECUTED`; không tuyên bố pass.

Validation tốt hơn ở API interval multiple 5m và DTO whitelist, nhưng timestamp/limit/threshold vẫn thiếu numeric range checks. Spark casts null/bad number thành zero. Error handling đôi khi nuốt lỗi (`BinanceAdapter` collector trả `[]`, signals repository trả `[]`), làm giảm khả năng phát hiện sự cố.
