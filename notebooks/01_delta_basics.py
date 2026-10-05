# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB1 — Delta Lake Basics (lightweight path)
#
# **Stack:** `deltalake` (delta-rs) + Polars + DuckDB. No Spark, no JVM.
# Maps to slide §2 (Delta Lake) + deliverable bullet 1.
#
# > Spark equivalent: `spark.read.format("delta").load(path)` ↔ `DeltaTable(path).to_pyarrow_table()`.
# > Same on-disk format, different binding.

# %%
import _setup  # noqa: F401  -- adds scripts/ to sys.path (file-relative)
import polars as pl
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset

table_path = path("scratch", "users_delta")
reset(table_path)  # idempotent rerun

# %% [markdown]
# ## 1. Write a Delta table

# %%
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "charlie"],
    "age": [30, 25, 35],
    "city": ["Hanoi", "HCMC", "Danang"],
})
write_deltalake(table_path, df.to_arrow(), mode="overwrite")

# %% [markdown]
# ## 2. Read it back + inspect transaction log
#
# Look at `_lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json` —
# that's the transaction log. Same JSON format Spark/Databricks would write.

# %%
dt = DeltaTable(table_path)
print(pl.from_arrow(dt.to_pyarrow_table()))
print("\nHistory:")
for h in dt.history():
    print(f"  v{h['version']}  {h['operation']}  {h.get('operationMetrics', {})}")

from pathlib import Path as _Path  # noqa: E402
_initial_log = sorted(_Path(table_path).glob("_delta_log/*.json"))[0]
print(f"\n--- Content of initial commit {_initial_log.name} ---")
with open(_initial_log) as f:
    for line in f:
        print("  " + line.strip())

# %% [markdown]
# ## 3. Schema enforcement — try to write a wrong schema

# %%
bad = pl.DataFrame({"id": [4], "name": ["dan"], "age": ["thirty"], "city": ["Hue"]})
bad_write_blocked = False
try:
    write_deltalake(table_path, bad.to_arrow(), mode="append")
    print("UNEXPECTED: bad write succeeded — schema enforcement broken")
except Exception as e:
    bad_write_blocked = True
    msg = str(e).splitlines()[0][:120]
    print(f"BLOCKED by schema enforcement (expected): {type(e).__name__}: {msg}")

# %% [markdown]
# ## 4. Schema evolution (opt-in)

# %%
new = pl.DataFrame({
    "id": [4], "name": ["dan"], "age": [28], "city": ["Hue"], "tier": ["premium"],
})
write_deltalake(table_path, new.to_arrow(), mode="append", schema_mode="merge")
dt = DeltaTable(table_path)
# Sort by id so the printout is stable across reruns — Delta does not
# preserve write-order across appends.
print(pl.from_arrow(dt.to_pyarrow_table()).sort("id"))

# %% [markdown]
# ## 5. Query with DuckDB via Arrow (part of the required notebook)

# %%
import duckdb

# We hand DuckDB an Arrow table rather than calling `delta_scan()`. delta_scan
# autoloads a DuckDB extension over the network — fine at home, a support
# ticket in a firewalled classroom. Arrow registration is zero-copy and offline.
con = duckdb.connect()
con.register("users", DeltaTable(table_path).to_pyarrow_table())
tier_counts = con.sql("SELECT tier, count(*) AS n FROM users GROUP BY 1 ORDER BY 1").fetchall()
print(tier_counts)

# %% [markdown]
# ## 6. Trả lời câu hỏi & Giải thích kết quả (Challenge 3.1)
#
# ### 1. Enforcement khác Evolution ở điểm nào?
# - **Schema Enforcement (Bảo vệ tính nhất quán - Gatekeeper)**: Là cơ chế phòng thủ tự động của Delta Lake. Khi một tiến trình ghi dữ liệu (append/overwrite), engine đối chiếu cấu trúc dữ liệu ghi vào với schema đã cam kết trong `_delta_log`. Bất kỳ vi phạm nào về kiểu dữ liệu (như ghi `age='thirty'` dạng String vào cột Int64) hoặc cấu trúc cột chưa được phép đều bị **chặn đứng lập tức** (`SchemaMismatchError`). Cơ chế này ngăn chặn tuyệt đối tình trạng rác hóa dữ liệu (silent data corruption) ở tầng lưu trữ.
# - **Schema Evolution (Tiến hóa cấu trúc có kiểm soát - Controlled Migration)**: Là cơ chế cho phép bảng thích nghi với sự thay đổi của nghiệp vụ theo thời gian (ví dụ: thêm cột mới `tier`). Thay vì phải viết lại bảng hoặc chạy các lệnh DDL phức tạp, Delta Lake tự động mở rộng schema và cập nhật metadata, nhưng chỉ diễn ra khi người dùng **chủ động bật cờ** (`schema_mode="merge"`).
#
# ### 2. Vì sao thêm cột cần opt-in (`schema_mode="merge"`)?
# - Nếu hệ thống tự động thêm cột mà không cần opt-in, các rủi ro nghiêm trọng sẽ xảy ra:
#   1. **Lỗi gõ phím (Typo)**: Một lỗi nhỏ trong mã nguồn upstream (ví dụ gửi trường `teir` hoặc `Tier` thay vì `tier`) sẽ vĩnh viễn tạo thêm cột rác vào bảng.
#   2. **Phá vỡ hợp đồng dữ liệu (Data Contract Violation)**: Các pipeline hoặc dashboard đọc dữ liệu phía sau (downstream consumers) thường dựa vào schema ổn định; việc tự động thay đổi cấu trúc bảng có thể làm sập các ứng dụng này.
#   3. **Tràn lan schema (Schema Bloat)**: Trong kiến trúc microservices nhiều nguồn cùng đẩy dữ liệu, không kiểm soát việc thêm trường sẽ làm bảng nhanh chóng mất kiểm soát.
#   Do đó, opt-in là nguyên tắc quản trị bắt buộc để bảo vệ tính toàn vẹn dữ liệu.
#
# ### 3. Transaction log cung cấp bằng chứng gì về một lần ghi?
# Transaction log (`_delta_log/*.json`) là nguồn chân lý duy nhất (Single Source of Truth) bảo đảm tính chất ACID cho Delta Lake. Trong mỗi commit JSON, transaction log cung cấp bằng chứng nguyên tử gồm:
# - **`commitInfo`**: Thời điểm ghi (`timestamp`), loại thao tác (`operation`: WRITE/APPEND/MERGE), công cụ ghi (`engineInfo`: delta-rs / Spark), và các số đo vận hành (`operationMetrics`: số dòng thêm/xóa, số file thêm/xóa, thời gian thực thi).
# - **`protocol`**: Phiên bản giao thức đọc/ghi tối thiểu yêu cầu cho client (`minReaderVersion`, `minWriterVersion`).
# - **`metaData`**: Bản chụp schema đầy đủ (`schemaString`), partition columns, định dạng file (`format`).
# - **`add` / `remove` actions**: Bằng chứng chính xác về các file vật lý: tên file Parquet được thêm (`add.path`) hoặc bị đánh dấu xóa (`remove.path`), dung lượng (`size`), và metadata thống kê ở cấp độ file (`stats`: `numRecords`, `minValues`, `maxValues`, `nullCount`) hỗ trợ data skipping khi truy vấn.

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] `_delta_log/` contains JSON files
# - [ ] Schema enforcement blocked the bad write
# - [ ] schema_mode="merge" added the `tier` column
# - [ ] DuckDB query returned 2 tier groups
# The final schema-enforcement flag is hardcoded; inspect the actual error
# from the bad-write cell rather than treating that PASS line as proof.

# %%
_log = sorted(_Path(table_path).glob("_delta_log/*.json"))
_cols = DeltaTable(table_path).schema().to_arrow().names
checks = {
    "_delta_log/ has JSON commits": len(_log) >= 2,
    "schema enforcement blocked bad write": bad_write_blocked,
    "tier column added via schema_mode=merge": "tier" in _cols,
    "duckdb sees 2 tier groups": len(tier_counts) == 2,
}
for k, v in checks.items():
    print(f"  [{'PASS' if v else 'FAIL'}] {k}")
assert all(checks.values()), "NB1 incomplete — see FAIL rows above"
print("\nNB1 complete.")
