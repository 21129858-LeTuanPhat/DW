package org.example;

import jakarta.mail.MessagingException;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, MessagingException {
        NintendoCompare process = new NintendoCompare();

        // 5.0.0 Đọc file cấu hình JSON, gán vào jsonConfig.
        process.getJsonConfig("D:\\DW\\configs.json");
        // 5.0.2 Kiểm tra trạng thái tiến trình.
        if (process.checkIfHasProcessAndNotError()) {
            return;
        }
        // 5.0.3 Chạy file jar để lấy thông tin task.
        process.getTask();
        // 5.0.4 Ghi log tiến trình vào DB, lấy processId.
        process.insertLogAndGetProcessId();
        // 5.0.5 Tạo kết nối DB từ thông tin trong task.
        process.getConnection();
        // 5.0.6 Thực thi stored procedure sync_nintendo_games() qua JDBC.
        process.doJob();
        // 5.0.7 Xuất dữ liệu ra file CSV
        process.exportToCsv();
        // 5.0.8 Cập nhật trạng thái log là DONE và đóng kết nối DB.
        process.finish();
    }
}