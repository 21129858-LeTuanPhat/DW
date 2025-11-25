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
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class NintendoCompare {
    private Connection connection;
    private Task task;
    private JsonConfig jsonConfig;
    private long processId = 0L;

    NintendoCompare() {
    }

    public void doJob() throws MessagingException {
        String sql = "{call sync_nintendo_games()}";

        try (CallableStatement stmt = connection.prepareCall(sql)) {
            stmt.execute();
        } catch (SQLException e) {
            // 5.7.0 Gửi email báo lỗi
            EmailHelper.Send("Lỗi Job Nintendo compare",
                    "Lỗi khi call proc sync_nintendo_games()",
                    "<h1 style=\"color: red;\">Error Error!!!</h1><p>" + e.getMessage() + "</p>");
            // 5.7.1 In ra log lỗi khi call proc sync_nintendo_games
            System.out.println("Lỗi khi call proc sync_nintendo_games");
            // 5.7.2 Cập nhật trạng thái process thành ERROR
            DbProcess.updateLog(processId, "ERROR");
            throw new RuntimeException(e);
        }
    }

    public void exportToCsv() throws MessagingException {
        String query = "SELECT * FROM " + "nintendo_games";

        try (
                Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);

                BufferedWriter writer = Files.newBufferedWriter(
                        Paths.get("D:\\DW\\datas\\nintendo_games.csv"),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING)) {

            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            for (int i = 1; i <= colCount; i++) {
                writer.write(meta.getColumnName(i));
                if (i < colCount)
                    writer.write(";");
            }
            writer.write("\n");

            while (rs.next()) {
                for (int i = 1; i <= colCount; i++) {

                    String value = rs.getString(i);
                    if (value == null)
                        value = "";

                    writer.write(value);

                    if (i < colCount)
                        writer.write(";");
                }
                writer.write("\n");
            }

        } catch (Exception e) {
            // 5.8.0 Gửi email báo lỗi
            EmailHelper.Send("Lỗi Job Nintendo compare",
                    "Lỗi export csv",
                    "<h1 style=\"color: red;\">Error Error!!!</h1><p>" + e.getMessage() + "</p>");
            // 5.8.1 In ra log Lỗi khi export csv
            System.out.println("Lỗi khi export csv");
            // 5.8.2 Cập nhật trạng thái process thành ERROR
            DbProcess.updateLog(processId, "ERROR");
            throw new RuntimeException(e);
        }

        System.out.println("Xuất CSV thành công:");
    }

    public void getTask() {
        ProcessBuilder pb = new ProcessBuilder(
                "java",
                "-jar",
                "D:\\DW\\file_jar\\LoadConfig1-1.0-SNAPSHOT.jar",
                "12", "2", "D:\\DW\\configs.json");

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
            // 5.4.0 Gửi email lỗi
            EmailHelper.Send("Lỗi Job Nintendo compare",
                    "Lỗi khi Parse Task",
                    "<h1 style=\"color: red;\">Error Error!!!</h1><p>" + e.getMessage() + "</p>");
            // 5.4.1 In log Lỗi khi Parse Task
            System.out.println("Lỗi khi Parse Task");
            throw new RuntimeException(e);
        }
    }

    public void getJsonConfig(String filePath) {
        try {
            this.jsonConfig = DbProcess.getConfigJson(filePath);
            // 5.0.1 Gọi DbProcess.setConfig gán config mới
            DbProcess.setConfig(this.jsonConfig);
        } catch (MessagingException e) {
            // 5.1.0 Gửi email lỗi
            EmailHelper.Send("Lỗi Job Nintendo compare",
                    "Lỗi khi Parse JsonConfig",
                    "<h1 style=\"color: red;\">Error Error!!!</h1><p>" + e.getMessage() + "</p>");
            // 5.1.1 In log lỗi khi Parse JsonConfig
            System.out.println("Lỗi khi Parse JsonConfig");
            throw new RuntimeException(e);
        }
    }

    public boolean checkIfHasProcessAndNotError() {
        if (DbProcess.checkHasProcessAndNotError()) {
            // 5.3.0 Gửi email lỗi
            EmailHelper.Send("Lỗi Job Nintendo compare",
                    "Không thể thực hiện tiến trình do bị tiến trình khác block",
                    "<h1 style=\"color: red;\">Error Error!!!</h1>");
            // 5.3.0 In log không thể thực hiện tiến trình do bị tiến trình khác block
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
            // 5.5.0 Gửi email lỗi
            EmailHelper.Send("Lỗi Job Nintendo compare",
                    "Lỗi khi insert Process",
                    "<h1 style=\"color: red;\">Error Error!!!</h1><p>" + e.getMessage() + "</p>");
            // 5.5.1 In ra log lỗi khi insert Process
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
            // 5.6.0 Gửi email lỗi
            EmailHelper.Send("Lỗi Job Nintendo compare",
                    "Lỗi khi kết nối tới DB",
                    "<h1 style=\"color: red;\">Error Error!!!</h1><p>" + e.getMessage() + "</p>");
            // 5.6.1 In ra log lỗi khi kết nối tới DB
            System.out.println("Lỗi khi kết nối tới DB");
            // 5.6.2 Cập nhật trạng thái process thành ERROR
            DbProcess.updateLog(processId, "ERROR");
            throw new RuntimeException(e);
        }
    }

    public void finish() {
        try {
            DbProcess.updateLog(processId, "DONE");
            this.connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
