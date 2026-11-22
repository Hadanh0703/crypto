# Frontend

React 19 SPA với routes `/overview`, `/chart`, `/news`, `/news-impact`, `/chatbot` (`App.tsx:11-21`). API base mặc định `/api`, mock chỉ khi `VITE_USE_MOCK=true` (`services/apiClient.ts:1-3`).

- Overview gọi song song summary, movers, spikes, signals, latest news (`OverviewPage.tsx:33-48`).
- Chart giữ symbol/interval trên query string; gọi server time, klines, indicators; signals lấy riêng. lightweight-charts v4 hiển thị candle/volume/RSI/overlays.
- News hỗ trợ paging và filters; normalize categories JSON hoặc CSV.
- News Impact gọi klines, markers, table, correlation; click marker gọi detail impact.
- Chatbot giữ history ở component nhưng request chỉ gửi message hiện tại; backend cũng không nhận history.

Các trang có loading/error state cục bộ. Không thấy polling interval tự động; refresh chủ yếu do mount/thay query (`VERIFIED_IN_CODE`). Frontend normalize nhiều response shape để chịu được interceptor/mock khác nhau. Nginx K8s proxy `/api/` tới backend và SPA fallback.
