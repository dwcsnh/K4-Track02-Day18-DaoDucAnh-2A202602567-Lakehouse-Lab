# Reflection: Anti-Pattern Lakehouse & Phòng Ngừa

**Anti-pattern: Small-File Problem do Streaming Ingestion**

Trong hệ thống streaming (LLM observability, telemetry), ghi micro-batch liên tục sinh ra hàng triệu file nhỏ (vài KB). Hậu quả: chi phí request `GET` trên S3 tăng vọt, metadata log phình to, và query bị nghẽn I/O khi scan.

**Biện pháp phòng ngừa:**
1. **Kiểm soát Ingest:** Tăng trigger window (1–5 phút) tại streaming buffer trước khi ghi lakehouse.
2. **Compaction & Z-Order:** Lập lịch định kỳ gom file về chuẩn 128–512 MB, kèm Z-order trên khóa lọc chính (`tenant_id`) để tăng skipping.
3. **Dọn rác:** Chạy `VACUUM` / `expire_snapshots` định kỳ (retention ≥ 7 ngày) kèm quét orphan files để giải phóng dung lượng đĩa.

*(Chi tiết khai báo AI: [AI_USAGE.md](AI_USAGE.md))*
