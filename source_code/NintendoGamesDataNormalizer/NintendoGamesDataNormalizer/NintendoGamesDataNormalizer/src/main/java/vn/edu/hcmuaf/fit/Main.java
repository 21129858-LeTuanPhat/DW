package vn.edu.hcmuaf.fit;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.mail.MessagingException;
import vn.edu.hcmuaf.fit.config.JsonConfig;
import vn.edu.hcmuaf.fit.config.Task;
import vn.edu.hcmuaf.fit.email.EmailHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(
                "java",
                "-jar",
                "D:\\DW\\file_jar\\LoadConfig1-1.0-SNAPSHOT.jar",
                "10", "3", "D:\\DW\\configs.json"
        );

        pb.redirectErrorStream(true);
        Process process = pb.start();
        Task task;
        JsonConfig jsonConfig;

        try {
            jsonConfig = DbProcess.getConfigJson("D:\\DW\\configs.json");
        } catch (MessagingException e) {
            EmailHelper.Send("Lỗi Job Nintendo compare",
                    "Lỗi khi Parse JsonConfig",
                    "<h1 style=\"color: red;\">Error Error!!!</h1><p>" + e.getMessage() +"</p>"
            );
            System.out.println("Lỗi khi Parse JsonConfig");
            throw new RuntimeException(e);
        }

        DbProcess.setConfig(jsonConfig);

        if (DbProcess.checkHasProcessAndNotError()) {
            EmailHelper.Send("Lỗi Job Nintendo compare",
                    "Không thể thực hiện tiến trình do bị tiến trình khác block",
                    "<h1 style=\"color: red;\">Error Error!!!</h1>"
            );
            System.out.println("Không thể thực hiện tiến trình do bị tiến trình khác block");
            return;
        }

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
            task = mapper.readValue(output.toString(), Task.class);

            System.out.println("Task ID: " + task.getId());
            System.out.println("Task name: " + task.getName());
            System.out.println("DB Server: " + task.getDbConfig().getServerIp());
            System.out.println("User: " + task.getDbConfig().getUser());
            System.out.println("Password: " + task.getDbConfig().getPassword());

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        long processId = 0;
        try {
            processId = DbProcess.insertLog(task.getName(), "PROCESSING", task.getDescription(), task.getId());
        } catch (Exception e) {
            EmailHelper.Send("Lỗi Job Nintendo compare",
                    "Lỗi khi insert Process",
                    "<h1 style=\"color: red;\">Error Error!!!</h1><p>" + e.getMessage() +"</p>"
            );
            System.out.println("Lỗi khi insert Process");
            throw new RuntimeException(e);
        }

        String serverIp = task.getDbConfig().getServerIp();
        int port = task.getDbConfig().getPort();
        String dbName = task.getDbConfig().getName();
        String username = task.getDbConfig().getUser();
        String password = task.getDbConfig().getPassword();

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s", serverIp, port, dbName);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            NintendoGamesDataNormalizer normalizer = new NintendoGamesDataNormalizer(conn, processId);

            System.out.println("=".repeat(60));
            System.out.println("Nintendo Games Data Normalizer (Using Stored Procedures)");
            System.out.println("=".repeat(60));

            normalizer.loadCsvFile(task.getSourcePath());

            // Normalize data
            normalizer.normalizeAndLoad();

            // In thống kê
            normalizer.printStatistics();

            DbProcess.updateLog(processId, "DONE");

        } catch (SQLException e) {
            System.err.println("\n✗ Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
