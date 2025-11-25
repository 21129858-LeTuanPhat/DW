package org.example;

import jakarta.mail.MessagingException;

public class Main {
    public static void main(String[] args) throws MessagingException {

        NintendoReloadFactGameGenres process = new NintendoReloadFactGameGenres();
        // 8.0.0 Đọc file cấu hình JSON, gán vào jsonConfig.
        process.getJsonConfig("D:\\DW\\configs.json");
        // 8.0.2 Kiểm tra trạng thái tiến trình.
        if (process.checkIfHasProcessAndNotError()) {
            return;
        }
        // 8.0.3 Chạy file jar để lấy thông tin task.
        process.getTask();
        // 8.0.4 Ghi log tiến trình vào DB, lấy processId.
        process.insertLogAndGetProcessId();
        // 8.0.5 Tạo kết nối DB từ thông tin trong task.
        process.getConnection();
        // 8.0.6 Thực thi stored procedure refresh_all_aggregates() qua JDBC.
        process.doJob();
        // 8.0.7 Đóng kết nối DB, cập nhật log trạng thái "DONE".
        process.finish();

    }
}