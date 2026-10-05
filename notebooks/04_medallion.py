# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB4 — Medallion Pipeline (Bronze → Silver → Gold), lightweight
#
# **Use case:** LLM observability — exact schema from slide §8 (Lakehouse cho AI/ML) medallion frame.
# Maps to deliverable bullet 4 (the Milestone-1 Lakehouse artifact).
#
# Pre-req: ran `make data` — but if you jumped straight here, the cell below
# generates the Bronze sample for you rather than failing on a missing path.

# %%
import _setup  # noqa: F401  -- adds scripts/ to sys.path
from pathlib import Path

import polars as pl
import duckdb
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset

BRONZE = path("bronze", "llm_calls_raw")
SILVER = path("silver", "llm_calls")
GOLD   = path("gold",   "llm_daily_metrics")

# Self-healing pre-req (same pattern as NB7/NB8). Without this, skipping
# `make data` surfaces as a raw `Os { code: 2, kind: NotFound }` from the Rust
# layer — technically correct, useless to a student.
if not Path(BRONZE).exists():
    print("Bronze not found — running scripts/generate_data_lite.py first ...")
    import generate_data_lite

    generate_data_lite.main()

# %% [markdown]
# ## Bronze — verify raw is loaded

# %%
bronze_n = DeltaTable(BRONZE).to_pyarrow_table().num_rows
print(f"Bronze rows: {bronze_n:,}")
print(pl.from_arrow(DeltaTable(BRONZE).to_pyarrow_table().slice(0, 2)))

# %% [markdown]
# ## Silver — parse, validate, dedup
#
# Rules: drop malformed JSON, dedupe by `request_id`, project typed columns.

# %%
reset(SILVER)

# DuckDB does the JSON parse + dedup in one query — Polars also works,
# DuckDB just has nicer JSON syntax for this case.
# DuckDB reads Delta through Arrow, not through `delta_scan()`. The latter
# autoloads an extension over the network; Arrow registration is offline and
# zero-copy, so the lab works on a locked-down machine.
con = duckdb.connect()
con.register("bronze", DeltaTable(BRONZE).to_pyarrow_table())

silver_arrow = con.sql(f"""
    WITH parsed AS (
      SELECT
        request_id,
        ts,
        CAST(ts AS DATE)                            AS date,
        json_extract_string(raw_json, '$.model')          AS model,
        json_extract_string(raw_json, '$.user_id')        AS user_id,
        CAST(json_extract(raw_json, '$.usage.input')  AS INTEGER) AS prompt_tokens,
        CAST(json_extract(raw_json, '$.usage.output') AS INTEGER) AS completion_tokens,
        CAST(json_extract(raw_json, '$.latency_ms')   AS INTEGER) AS latency_ms,
        json_extract_string(raw_json, '$.status')         AS status,
        ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts) AS rn
      FROM bronze
    )
    SELECT request_id, ts, date, model, user_id,
           prompt_tokens, completion_tokens, latency_ms, status
    FROM parsed
    WHERE rn = 1 AND model IS NOT NULL
""").arrow()

write_deltalake(SILVER, silver_arrow, mode="overwrite", partition_by=["date"])

silver_n = DeltaTable(SILVER).to_pyarrow_table().num_rows
print(f"Silver rows: {silver_n:,}  (Bronze {bronze_n:,} → dedup dropped {bronze_n - silver_n:,})")
assert silver_n < bronze_n, (
    "Silver has the same row count as Bronze — dedup did not run. "
    "Did you regenerate Bronze with the latest generator (which injects retries)?"
)

# %% [markdown]
# ## Gold — aggregate to (date, model) metrics

# %%
reset(GOLD)

# Illustrative cost model — NOT canonical pricing.
# (input USD / 1M tokens, output USD / 1M tokens)
COST_TABLE = """
  VALUES
    ('claude-haiku-4-5',  0.80,  4.00),
    ('claude-sonnet-4-6', 3.00, 15.00),
    ('claude-opus-4-7', 15.00, 75.00)
"""

con.register("silver", DeltaTable(SILVER).to_pyarrow_table())
gold_arrow = con.sql(f"""
    WITH cost(model, c_in, c_out) AS ({COST_TABLE})
    SELECT
      s.date,
      s.model,
      QUANTILE_CONT(s.latency_ms, 0.50) AS p50_latency_ms,
      QUANTILE_CONT(s.latency_ms, 0.95) AS p95_latency_ms,
      SUM(s.prompt_tokens)              AS total_prompt_tokens,
      SUM(s.completion_tokens)          AS total_completion_tokens,
      AVG(CASE WHEN s.status <> 'ok' THEN 1.0 ELSE 0.0 END) AS error_rate,
      (SUM(s.prompt_tokens)     * c.c_in  / 1e6) +
      (SUM(s.completion_tokens) * c.c_out / 1e6) AS cost_usd
    FROM silver s
    JOIN cost c USING (model)
    GROUP BY s.date, s.model, c.c_in, c.c_out
    ORDER BY s.date, s.model
""").arrow()

write_deltalake(GOLD, gold_arrow, mode="overwrite", partition_by=["date"])

# Z-order for fast filter-by-model dashboards
DeltaTable(GOLD).optimize.z_order(["model"])

# %% [markdown]
# ## Verify Gold

# %%
gold_df = pl.from_arrow(DeltaTable(GOLD).to_pyarrow_table())
print(gold_df)

# Slide-5 deliverable: "Gold p50/p95/cost qua ≥ 7 ngày". Make that explicit.
n_dates = gold_df.select("date").n_unique()
n_models = gold_df.select("model").n_unique()
print(
    f"\n──── Gold deliverable metrics ────\n"
    f"  Distinct dates:   {n_dates:>3}   (target ≥ 7)\n"
    f"  Distinct models:  {n_models:>3}\n"
    f"  Total Gold rows:  {gold_df.height:>3}   (= dates × models)"
)
assert n_dates >= 7, (
    f"Gold has only {n_dates} dates — slide deliverable requires ≥ 7. "
    "Re-run `make data` (the generator spreads across 7 UTC days)."
)

# %% [markdown]
# ## 5. Trả lời câu hỏi & Giải thích kết quả (Challenge 3.4)
#
# ### 1. Dedup ở Silver giải quyết vấn đề nào?
# - Trong kiến trúc dữ liệu thực tế (đặc biệt là LLM observability, event streaming từ Kafka/Kinesis), các sự kiện được gửi theo cơ chế **at-least-once delivery**. Do mạng chập chờn, timeout, hoặc client retry, cùng một `request_id` thường xuyên bị gửi nhiều lần vào tầng Bronze (trong dữ liệu lab: Bronze có 200,000 dòng nhưng chỉ có ~190,052 unique `request_id`, tức ~9,948 bản ghi trùng lặp).
# - Việc khử trùng lặp (deduplication) bằng cửa sổ `ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts)` tại tầng Silver giúp loại bỏ hoàn toàn các bản ghi trùng lặp, đảm bảo mỗi yêu cầu suy luận chỉ được tính duy nhất một lần. Nếu không dedup ở Silver, các tầng phân tích phía sau sẽ tính sai lệch nghiêm trọng: thổi phồng số lượng request, tính sai tổng token tiêu thụ, phóng đại chi phí hóa đơn và sai lệch phân phối latency.
#
# ### 2. Vì sao dashboard đọc Gold thay vì đọc thẳng Silver hay Bronze?
# 1. **Hiệu năng và độ trễ truy vấn (Query Latency)**: Bảng Gold là tập hợp các chỉ số đã được tiền tổng hợp (pre-aggregated) theo chiều phân tích nghiệp vụ (`date`, `model`). Dung lượng bảng Gold chỉ có 21 dòng (7 ngày × 3 model) so với hàng trăm ngàn dòng ở Silver và Bronze. Truy vấn dashboard trên Gold phản hồi tức thì (< 5ms) thay vì phải scan và tính toán lại từ đầu hàng triệu dòng.
# 2. **Tiết kiệm tài nguyên và chi phí FinOps**: Việc hàng chục người dùng hoặc ứng dụng BI liên tục refresh dashboard sẽ gây lãng phí CPU/RAM khổng lồ nếu mỗi lần xem lại phải parse JSON và chạy tính quantile trên hàng trăm nghìn dòng. Bảng Gold tính toán 1 lần duy nhất trong pipeline định kỳ.
# 3. **Chuẩn hóa logic nghiệp vụ (Single Source of Truth)**: Định nghĩa về tỷ lệ lỗi (`error_rate`), phân vị trễ (`p50`, `p95`), công thức tính chi phí token được tập trung hóa tại pipeline tạo Gold, tránh tình trạng mỗi dashboard viết một công thức SQL khác nhau gây mâu thuẫn số liệu.
# 4. **Tối ưu hóa layout**: Bảng Gold được tối ưu hóa chuyên biệt cho dashboard (ví dụ: partition theo `date`, Z-order theo `model`) để phục vụ các bộ lọc phổ biến của báo cáo.
#
# ### 3. Cách tính error rate và chi phí trong query có phù hợp với dữ liệu đầu vào không?
# - **Về Error Rate**:
#   - Công thức: `AVG(CASE WHEN s.status <> 'ok' THEN 1.0 ELSE 0.0 END)`.
#   - Phù hợp hoàn hảo vì trường `status` nhận giá trị `'ok'` khi thành công và các mã lỗi (như `'rate_limit'`, `'timeout'`, `'server_error'`) khi thất bại. Giá trị trung bình của biến chỉ thị 0/1 này trả về đúng tỷ lệ lỗi xác suất trong đoạn `[0, 1]`.
# - **Về Chi phí (cost_usd)**:
#   - Công thức: `(SUM(prompt_tokens) * c_in / 1e6) + (SUM(completion_tokens) * c_out / 1e6)` với bảng giá tương ứng cho từng model.
#   - Phù hợp với mô hình kinh doanh và cách tính cước của các API LLM hiện nay (Anthropic, OpenAI): giá được tính riêng cho input token và output token theo đơn vị triệu tokens (per 1M tokens).

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] All three tables exist under `_lakehouse/{bronze,silver,gold}/`
# - [ ] Silver has fewer rows than Bronze (dedup worked)
# - [ ] Gold spans ≥ 7 dates × 3 models (slide §8 medallion contract)
# - [ ] Cost & error_rate columns populated and non-zero
# - [ ] p50 <= p95, cost_usd > 0, error_rate in [0, 1] across all Gold rows

# %%
p50_le_p95 = (gold_df["p50_latency_ms"] <= gold_df["p95_latency_ms"]).all()
cost_positive = (gold_df["cost_usd"] > 0).all()
error_rate_valid = ((gold_df["error_rate"] >= 0.0) & (gold_df["error_rate"] <= 1.0)).all()

checks = {
    "all 3 tables exist on storage": Path(BRONZE).exists() and Path(SILVER).exists() and Path(GOLD).exists(),
    "silver dedup dropped rows":     silver_n < bronze_n,
    "gold spans ≥ 7 dates":          n_dates >= 7,
    "gold covers 3 models":          n_models == 3,
    "gold p50 <= p95 latency":       bool(p50_le_p95),
    "gold cost_usd strictly > 0":    bool(cost_positive),
    "gold error_rate in [0, 1]":     bool(error_rate_valid),
}
for k, v in checks.items():
    print(f"  [{'PASS' if v else 'FAIL'}] {k}")
assert all(checks.values()), "NB4 incomplete — see FAIL rows above"
print("\nNB4 complete.")
