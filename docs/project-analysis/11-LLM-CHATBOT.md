# LLM / Chatbot

Backend dùng OpenAI SDK, model mặc định `gpt-4o-mini`, optional `OPENAI_BASE_URL`, key từ `OPENAI_API_KEY` (`chatbot.service.ts:30-46`). Nếu thiếu key trả 503. POST `/api/chatbot` nhận một `message`.

Service xây system prompt tiếng Việt và function schemas, cho model chọn tool; tối đa nhiều round, chạy tool calls song song và hard-cap payload 30 items. Tools map sang REST endpoints nội bộ, gồm klines, news và signals (`chatbot.service.ts:23-127,187-319`). LLM không truy cập DB trực tiếp; backend gọi lại chính API qua `INTERNAL_API_BASE`. LLM vừa chọn dữ liệu vừa diễn giải kết quả, nhưng phép tính đáng tin cậy vẫn nên nằm trong API.

Không có persistence/history server-side, token budget theo hội thoại, RAG, embeddings, vector DB, citations kiểm chứng, prompt-injection filtering hay per-tool authorization (`NOT_IMPLEMENTED`). Internal endpoint không có auth, và raw news body có thể mang instruction độc hại vào context. Logging ghi tool args; tránh đưa secret vào arguments.
