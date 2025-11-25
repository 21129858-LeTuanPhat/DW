package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.mail.MessagingException;
import org.example.config.JsonConfig;
import org.example.config.Task;
import org.example.email.EmailHelper;

public class NintendoReloadFactGameGenres {
    private long processId = 0L;
    private Task task;
    private JsonConfig jsonConfig;
    private Connection connection;

    NintendoReloadFactGameGenres() {
    }

    public void doJob() throws MessagingException {
        String sql = "{call refresh_all_aggregates()}";

        try (CallableStatement stmt = connection.prepareCall(sql)) {
            stmt.execute();
            System.out.println("reload_fact_genre_games DONE");
        } catch (SQLException e) {
            // 8.7.0 Gửi email lỗi.
            EmailHelper.Send("Lỗi Job Nintendo Reload Fact Genre Games",
                    "Lỗi khi call proc refresh_all_aggregates()",
                    "<h1 style=\"color: red;\">Error Error!!!</h1><p>" + e.getMessage() + "</p>");
            DbProcess.updateLog(processId, "ERROR");
            // 8.7.1 In ra log Lỗi khi call proc refresh_all_aggregates
            System.out.println("Lỗi khi call proc refresh_all_aggregates");
            // 8.7.2 Cập nhật trạng thái process thành ERROR
            DbProcess.updateLog(processId, "ERROR");
            throw new RuntimeException(e);
        }
    }

    public void getTask() {
        ProcessBuilder pb = new ProcessBuilder(
                "java",
                "-jar",
                "D:\\DW\\file_jar\\LoadConfig1-1.0-SNAPSHOT.jar",
                "17", "3", "D:\\DW\\configs.json");

        pb.redirectErrorStream(true);
        try {
            Process process = pb.start();
            this.task = parseTaskFromProcess(process);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Task parseTaskFromProcess(Process process) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream()))) {

            String line;
            StringBuilder output = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }

            int exitCode = process.waitFor();
            System.out.println("Exit code: " + exitCode);
            System.out.println("Output:");
            System.out.println(output.toString());

            ObjectMapper mapper = new ObjectMapper();
            Task task = mapper.readValue(output.toString(), Task.class);

            System.out.println("Task ID: " + task.getId());
            System.out.println("Task name: " + task.getName());
            System.out.println("DB Server: " + task.getDbConfig().getServerIp());
            System.out.println("User: " + task.getDbConfig().getUser());
            System.out.println("Password: " + task.getDbConfig().getPassword());
            return task;

        } catch (InterruptedException | IOException e) {
            // 8.4.0 Gửi email lỗi
            EmailHelper.Send("Lỗi Job Nintendo Reload Fact Genre Games",
                    "Lỗi khi Parse Task",
                    "<h1 style=\"color: red;\">Error Error!!!</h1><p>" + e.getMessage() + "</p>");
            // 8.4.1 In log Lỗi khi Parse Task
            System.out.println("Lỗi khi Parse Task");
            throw new RuntimeException(e);
        }
    }

    public void getJsonConfig(String filePath) {
        try {
            this.jsonConfig = DbProcess.getConfigJson(filePath);
            // 8.0.1 Gọi DbProcess.setConfig gán config mới
            DbProcess.setConfig(this.jsonConfig);
        } catch (MessagingException e) {
            // 8.1.0. Gửi email lỗi
            EmailHelper.Send("Lỗi Job Nintendo Reload Fact Genre Games",
                    "Lỗi khi Parse JsonConfig",
                    "<h1 style=\"color: red;\">Error Error!!!</h1><p>" + e.getMessage() + "</p>");
            // 8.1.1 In log lỗi khi Parse JsonConfig
            System.out.println("Lỗi khi Parse JsonConfig");
            throw new RuntimeException(e);
        }
    }

    public boolean checkIfHasProcessAndNotError() {
        if (DbProcess.checkHasProcessAndNotError()) {
            // 8.3.0 Gửi email lỗi
            EmailHelper.Send("Lỗi Job Nintendo Reload Fact Genre Games",
                    "Không thể thực hiện tiến trình do bị tiến trình khác block",
                    "<h1 style=\"color: red;\">Error Error!!!</h1>");
            // 8.3.1 In log không thể thực hiện tiến trình do bị tiến trình khác block
            System.out.println("Không thể thực hiện tiến trình do bị tiến trình khác block");
            return true;
        }
        return false;
    }

    public void insertLogAndGetProcessId() {
        try {
            this.processId = DbProcess.insertLog(this.task.getName(), "PROCESSING", this.task.getDescription(),
                    this.task.getId());
        } catch (Exception e) {
            // 8.5.0 Gửi email lỗi
            EmailHelper.Send("Lỗi Job Nintendo Reload Fact Genre Games",
                    "Lỗi khi insert Process",
                    "<h1 style=\"color: red;\">Error Error!!!</h1><p>" + e.getMessage() + "</p>");
            // 8.5.1 In ra log lỗi khi insert Process
            System.out.println("Lỗi khi insert Process");
            throw new RuntimeException(e);
        }
    }

    public void getConnection() {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s", this.task.getDbConfig().getServerIp(),
                this.task.getDbConfig().getPort(), this.task.getDbConfig().getName());
        try {
            this.connection = DriverManager.getConnection(jdbcUrl, this.task.getDbConfig().getUser(),
                    this.task.getDbConfig().getPassword());
        } catch (SQLException e) {
            // 8.6.0 Gửi email lỗi
            EmailHelper.Send("Lỗi Job Nintendo Reload Fact Genre Games",
                    "Lỗi khi kết nối tới DB",
                    "<h1 style=\"color: red;\">Error Error!!!</h1><p>" + e.getMessage() + "</p>");
            // 8.6.1 In ra log lỗi khi kết nối tới DB
            System.out.println("Lỗi khi kết nối tới DB");
            // 8.6.2 Cập nhật trạng thái process thành ERROR
            DbProcess.updateLog(processId, "ERROR");
            throw new RuntimeException(e);
        }
    }

    public void finish() {
        try {
            this.connection.close();
            DbProcess.updateLog(processId, "DONE");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
