# Reflection — Top 5 Lakehouse Anti-Patterns

Anti-pattern lớn với dữ liệu quan sát LLM là coi vector index bên ngoài là nguồn dữ liệu chính. Khi người dùng yêu cầu xóa dữ liệu, bảng lakehouse có thể đã xóa row, nhưng index upsert vẫn giữ embedding cũ và có thể trả nội dung đó vào RAG. NB7 tái hiện tình huống này: sau khi xóa 8 tài liệu của `user_042`, bảng còn 0 hit nhưng external index vẫn trả 8 hit.

Tôi sẽ coi lakehouse là system of record và index là derived artifact có thể rebuild. Pipeline đồng bộ phải đọc Change Data Feed, xử lý delete, và lưu checkpoint/version để biết index đã theo kịp bảng hay chưa. Retention và VACUUM cũng phải được thiết kế cùng luồng xóa: xóa ở version hiện tại không tự động xóa dữ liệu còn được time travel tham chiếu. Nhờ vậy, rủi ro compliance được quan sát thay vì chỉ phát hiện sau sự cố.

AI hỗ trợ: dùng để hướng dẫn thực thi lab và diễn đạt phần reflection; số liệu trong notebook là output thực thi tại máy này.
