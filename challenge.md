2.3. Mở notebook đúng cách
Repo lưu notebook nguồn dưới dạng Jupytext .py. Bước cài đặt/chuyển đổi tạo các file .ipynb để mở trong Jupyter.

Trong Jupyter, mở 01_delta_basics.ipynb, chọn kernel của môi trường .venv vừa cài và thực thi lần lượt các cell từ trên xuống dưới. Nếu có lỗi import thư viện, kiểm tra lại môi trường kernel.

Lặp lại cách làm này với 8 notebook trong mục 3. Sau mỗi notebook:

1. Xem output thực tế và đối chiếu ngưỡng yêu cầu.
2. Thêm Markdown cell giải thích kết quả và trả lời câu hỏi của bước đó.
3. Lưu notebook có output, chụp ít nhất một ảnh thể hiện kết quả chính.


Thực hiện thử thách
3.1. Delta basics — NB1
Mở: 01_delta_basics.ipynb.

1. Chạy cell tạo bảng users_delta, đọc lại dữ liệu và xem history.
2. Mở commit JSON trong _lakehouse/scratch/users_delta/_delta_log/; quan sát các entry metadata và file được ghi.
3. Chạy cell ghi age='thirty'; ghi nhận lỗi chặn dữ liệu sai kiểu.
4. Chạy cell thêm cột tier bằng schema_mode="merge" và xem schema mới.
5. Chạy truy vấn DuckDB để đếm các nhóm tier.
Đoạn mã trọng tâm trong notebook:

write_deltalake(table_path, new.to_arrow(), mode="append", schema_mode="merge")
dt = DeltaTable(table_path)
print(dt.schema())
Chép
Tự kiểm tra: có ít nhất 2 commit JSON; bad write bị chặn; cột tier xuất hiện và truy vấn trả 2 nhóm tier.

Giải thích: enforcement khác evolution ở điểm nào? Vì sao thêm cột cần opt-in? Transaction log cung cấp bằng chứng gì về một lần ghi?

Bằng chứng: ảnh nội dung commit JSON, lỗi bad write và schema sau evolution. Cờ enforcement ở cuối notebook đang hardcoded; phải kiểm tra lỗi thật ở cell ghi sai kiểu.

3.2. Compaction và Z-order — NB2
Mở: 02_optimize_zorder.ipynb.

1. Chạy các cell tạo small files; ghi lại files_before và thời gian truy vấn ban đầu.
2. Chạy compaction và Z-order theo user_id với target size của notebook.
3. Chạy lại benchmark, ghi files_after, speedup và pruning ratio.
4. Xem min/max user_id của các file sau tối ưu và số file chứa target user.
Đoạn mã trọng tâm:

TARGET_SIZE = 256 * 1024
dt = DeltaTable(table_path)
dt.optimize.compact(target_size=TARGET_SIZE)
dt.optimize.z_order(["user_id"], target_size=TARGET_SIZE)
Chép
Tự kiểm tra: ít nhất 100 file trước tối ưu; số file giảm; speedup ≥ 3× hoặc pruning ratio ≥ 10×. Không bắt buộc đạt cả hai ngưỡng.

Giải thích: compaction và Z-order tác động khác nhau thế nào? Vì sao gộp thành một file lớn có thể làm khó quan sát file pruning? Vì sao thời gian benchmark biến động giữa các máy?

Bằng chứng: bảng số trước/sau và ảnh các metric cuối notebook. Dùng số đo của bạn, không thay bằng số minh họa trong tài liệu.

3.3. MERGE, time travel và RESTORE — NB3
Mở: 03_time_travel.ipynb.

1. Chạy các cell tạo dữ liệu và schema evolution để có các version ban đầu.
2. Chạy MERGE 100K dòng; quan sát các trường hợp update và insert.
3. Ghi dữ liệu lỗi có score < 0, xem history và query version cũ.
4. Chạy RESTORE về version 2; kiểm tra lại bảng hiện tại và history cuối cùng.
dt = DeltaTable(table_path)
dt.restore(2)
final_history = DeltaTable(table_path).history()
Chép
Tự kiểm tra: MERGE thành công; history có ít nhất 5 version, gồm RESTORE; số dòng score < 0 sau restore bằng 0.

Giải thích: đọc version cũ khác RESTORE thế nào? Vì sao RESTORE tạo transaction mới thay vì xóa lịch sử?

Bằng chứng: output MERGE, history sau RESTORE và số dòng lỗi còn lại.

3.4. Pipeline Bronze → Silver → Gold — NB4
Mở: 04_medallion.ipynb.

1. Quan sát dữ liệu Bronze LLM observability đã sinh ở bước chuẩn bị.
2. Chạy parse, chuẩn hóa và dedup để tạo Silver.
3. Ghi lại số dòng Bronze và Silver.
4. Chạy tổng hợp Gold theo ngày và model; kiểm tra p50/p95 latency, cost_usd và error_rate.
5. Xác định vị trí ba bảng trên storage và chụp kết quả Gold.
Tự kiểm tra: đủ Bronze/Silver/Gold; Silver ít dòng hơn Bronze; Gold phủ ít nhất 7 ngày × 3 model, p50 ≤ p95, chi phí dương, error rate nằm trong [0, 1]. Giá tính chi phí là giá minh họa của lab.

Giải thích: dedup ở Silver giải quyết vấn đề nào? Vì sao dashboard đọc Gold? Cách tính error rate và chi phí trong query có phù hợp với dữ liệu đầu vào không?

Bằng chứng: số dòng Bronze/Silver và Gold theo ngày × model. Notebook chưa assert toàn bộ điều kiện Gold; tự đối chiếu từng điều kiện.

3.5. Iceberg và catalog — NB5
Mở: 05_iceberg_catalog.ipynb.

1. Tạo bảng qua SQLite catalog; xem namespace, schema và partition spec day(ts).
2. So sánh số file của full scan với scan lọc trên ts, không lọc trực tiếp bằng cột partition dẫn xuất.
3. Xem metadata tree: metadata JSON, manifest list và manifests; ghi tỷ lệ metadata:data.
4. Đổi tên field sang latency_millis; so sánh field ID trước/sau.
5. Thay partition spec, append dữ liệu và đọc lại toàn bộ bảng.
Tự kiểm tra: pruning ratio ≥ 5×; field ID của latency_millis vẫn là 4; ít nhất 2 spec ID cùng tồn tại và bảng vẫn đọc được.

Giải thích: hidden partitioning hỗ trợ filter trên cột nguồn như thế nào? Field ID giúp gì khi rename? Vì sao partition evolution không yêu cầu mọi file cũ đổi layout ngay lập tức?

Bằng chứng: scan planning, metadata ratio, field ID và spec IDs. Notebook dùng catalog cục bộ và client-side planning.

3.6. Maintenance — NB6
Mở: 06_maintenance.ipynb.

Chạy theo thứ tự các job và ghi số đo trước/sau:

Job	Công việc	Kết quả cần kiểm tra
1	Compaction	Số file giảm ít nhất 10×
2	Clustering	Ít nhất 50% file có thể skip cho point query, chứng minh bằng min/max
3	Vacuum và snapshot expiry	Delta thu hồi bytes; Iceberg còn 3 snapshots
4	Orphan removal	Tìm và xóa 3 Delta orphan; dọn manifest lists Iceberg không còn được tham chiếu
5	Checkpoint	Có *.checkpoint.parquet và _last_checkpoint
Giải thích: vì sao orphan chưa từng commit có thể không được Delta vacuum dọn? Vì sao giảm snapshot trong đường PyIceberg này chưa đồng nghĩa file vật lý đã bị xóa? Retention ảnh hưởng reader cũ thế nào?

Bằng chứng: số file/bytes/snapshots trước và sau, kết quả orphan sweep và checkpoint. Kiểm tra dữ liệu hiện tại còn đọc được.

[!WARNING] Retention 0 trong notebook chỉ dùng với dữ liệu scratch của lab. Không chạy make clean khi notebook đang thực thi: lệnh này xóa venv và dữ liệu lakehouse cục bộ.
3.7. Multimodal và vectors — NB7
Mở: 07_vectors_multimodal.ipynb.

1. So sánh blob inline với pointer; đo amplification khi random-read và liên hệ row-group granularity.
2. Xem embeddings float32 được lưu/đọc qua Delta và cách cast khi query DuckDB.
3. Chạy quantization int8; ghi tỷ lệ dung lượng, recall@10 và topic fidelity.
4. Chạy SQL semantic search; xem các kết quả có cùng topic với query không.
5. Xóa dữ liệu trong bảng, truy vấn bảng và external index cũ để tái hiện lifecycle bug; quan sát CDF delete events.
Tự kiểm tra: amplification ≥ 5×; int8 nhỏ ít nhất 3×; recall@10 ≥ 0.80; topic fidelity ≥ 0.95; dữ liệu đã xóa có 0 hits trong bảng nhưng vẫn có hits ở index cũ.

Giải thích: tiết kiệm dung lượng đánh đổi chất lượng tìm kiếm ra sao? Recall theo doc ID khác topic fidelity thế nào? External index cần nhận loại sự kiện nào để hết trả dữ liệu đã xóa?

Bằng chứng: metric dung lượng/chất lượng và kết quả hai truy vấn sau delete.

3.8. Agents, version pin và provenance — NB8
Mở: 08_agents_provenance.ipynb.

1. Chạy trajectory medallion; xem Silver partition theo agent_version và Gold cho hai policy.
2. Pin version cho training run; append thêm dữ liệu rồi replay version đã pin.
3. Chạy lớp MCP mô phỏng: gọi list_tables 5 lượt, thử destructive call chưa xác nhận, rồi theo dõi task đến khi hoàn thành.
4. Xem cột provenance, 4 bucket minh họa và partition UNCLASSIFIED; kiểm tra bộ lọc loại UNCLASSIFIED khỏi tập trainable của lab.
5. Xóa subject và so sánh bảng hiện tại với version cũ.
Tự kiểm tra: Silver có 2 partition agent version; Gold có 2 policy; replay khớp số bước đã ghi; 5 lượt list chỉ đọc catalog 1 lần; call chưa xác nhận trả input_required; task hoàn thành; subject còn 0 dòng ở version hiện tại.

Giải thích: pin version giải quyết vấn đề gì? Vì sao xóa ở version hiện tại chưa xóa bản cũ? Những điểm nào khiến mô phỏng này chưa phù hợp làm cơ chế kiểm soát production?

Bằng chứng: version và số bước replay, số catalog reads, confirmation/task output, partitions và số dòng subject trước/sau.

[!NOTE] NB8 chỉ mô phỏng offline: replay hiện so số bước, chưa so toàn bộ nội dung; cờ confirmed do bên gọi truyền. Mapping provenance cũng chỉ minh họa, không chứng minh quyền dùng dữ liệu hay tuân thủ pháp luật. Xem các giới hạn cụ thể trong CHECKPOINTS.
3.9. Bonus tùy chọn — thiết kế lakehouse của riêng bạn
Chọn một tình huống trong Bonus Challenge và viết submission/bonus/ARCHITECTURE.md, dài 3–6 trang khi render.

Bài cá nhân cần có vấn đề và ràng buộc, một sơ đồ kiến trúc, ít nhất 5 quyết định với 2 alternatives bị loại cho mỗi quyết định, ít nhất 4 concept Day18 được áp dụng, 3 failure modes, phép tính storage/compute và kế hoạch MVP một tuần. Code PoC tùy chọn.

Phần bắt buộc có 100 điểm; bonus cộng tối đa 10 điểm, điểm lab cuối cùng tối đa 100. Không làm bonus không mất điểm phần bắt buộc.

Nội dung này có hữu ích không?