package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.example.config.Task;
import org.example.service.MailService;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDate;

public class LoadNintendoRaw {

    private static final int BATCH_SIZE = 1000;

    // Input params & process objects
    private Task task;
    private MailService mailService = new MailService();
    private String today;
    private String csvFilePath;
    private String jobName;

    // DB connections & log
    private Connection conn;
    private Connection connControl;
    private long logId = -1;
    private Process psLoad;

    public static void main(String[] args) throws SQLException {
        LoadNintendoRaw process = new LoadNintendoRaw();

        // 2.1.0 Thực thi process để load config
        process.executeLoadConfig();

        // 2.1.1 Đọc kết quả JSON trả về từ process Load config
        process.readTaskFromProcess();

        // 2.1.2 Xác định đường dẫn file CSV nguồn
        process.prepareCsvAndJobName();

        // 2.1.3 Mở kết nối đến cơ sở dữ liệu staging
        process.openConnections();

        // 2.1.4 Kiểm tra đủ điều kiện cho phép để thực hiện Load không
        process.checkLoadCondition();

        // 2.1.5 Bắt đầu ghi log vào process_log, trạng thái PROCESSING
        process.startProcessLog();

        // 2.1.6 Gọi hàm createTableIfNotExists để tạo table stg_nintendo_games_raw nếu chưa có
        process.createTableIfNotExistsWrapper();

        // 2.1.7 Gọi hàm truncateTable để xóa hết dữ liệu hiện tại trong table nếu có
        process.truncateTableWrapper();

        // 2.1.8 Gọi hàm loadCsvIntoTable để thực thi insert vào table
        process.loadCsvIntoTableWrapper();

        // 2.1.9 Cập nhật process hiện tại thành DONE
        process.logEnd(process.conn, process.logId, "DONE", "Load thành công ");

        //2.1.10 Hoàn thành process
        process.finish();
    }

    // ======================== 2.1.0 Thực thi process để load config ==========================
    private void executeLoadConfig() {
        try {
            ProcessBuilder pb = new ProcessBuilder("java", "-jar",
                    "D:\\DW\\file_jar\\LoadConfig1-1.0-SNAPSHOT.jar",
                    "7", "2", "D:\\DW\\configs.json"
            );
            pb.redirectErrorStream(true);
            this.psLoad = pb.start();


        } catch (Exception e) {
            // 2.2.1 In ra lỗi khi load config
            e.printStackTrace();
            // 2.2.2 Thực hiện gửi mail thông báo lỗi load config
            mailService.sendMailTo(
                    "22130217@st.hcmuaf.edu.vn",
                    "[DW][ERROR] Không thể chạy LoadConfig ",
                    "Không thể thực thi LoadConfig trên máy. Vui lòng kiểm tra đường dẫn," +
                            " quyền truy cập file và tham số truyền vào."
            );
            System.exit(1);
        }
    }

    // ======================== 2.1.1 Đọc kết quả JSON trả về từ process ==========================


    private Task readTaskFromProcess() {
        Task t = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(psLoad.getInputStream()))) {
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
            ObjectMapper mapper = new ObjectMapper();
            t = mapper.readValue(output.toString(), Task.class);

        } catch (Exception e) {
            // 2.3.2 Lỗi khi đọc dữ liệu json từ config
            e.printStackTrace();
            // 2.3.3 Thực hiện gửi mail thông báo lỗi khi đọc file
            mailService.sendMailTo(
                    "22130217@st.hcmuaf.edu.vn",
                    "[DW][ERROR] Lỗi đọc/parse JSON từ LoadConfig",
                    "Không thể đọc hoặc parse JSON trả về từ LoadConfig1-1.0-SNAPSHOT.jar. Lỗi: " +
                            e.getMessage());
            System.exit(1);
        }
        return t;
    }

    // ======================== 2.1.2 Xác định đường dẫn file CSV nguồn ==========================
    private void prepareCsvAndJobName() {
        today = LocalDate.now().toString();
        csvFilePath = "D:\\DW\\datas\\nintendo_details_" + today + ".csv";
        jobName = "Load Staging Nintendo (Job " + task.getId() + ")";
    }

    // ======================== 2.1.3 Mở kết nối đến cơ sở dữ liệu staging ==========================
    private void openConnections() {
        try {
            String serverIp = task.getDbConfig().getServerIp();
            int port = task.getDbConfig().getPort();
            String dbName = task.getDbConfig().getName();
            String username = task.getDbConfig().getUser();
            String password = task.getDbConfig().getPassword();
            String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s", serverIp, port, dbName);
            String jdbcUrlControl = String.format("jdbc:mysql://%s:%d/%s", serverIp, port, "control");

            conn = DriverManager.getConnection(jdbcUrl, username, password);
            connControl = DriverManager.getConnection(jdbcUrlControl, username, password);

        } catch (Exception e) {
            e.printStackTrace();
            mailService.sendMailTo(
                    "22130217@st.hcmuaf.edu.vn",
                    "[DW][ERROR] Lỗi kết nối cơ sở dữ liệu",
                    "Không thể kết nối đến database cho task id = " + (task != null ? task.getId() : "unknown") +
                            ". Lỗi: " + e.getMessage()
            );
            System.exit(1);
        }
    }

    // ======================== 2.1.4 Kiểm tra đủ điều kiện cho phép để thực hiện Load không ==========================
    private void checkLoadCondition() {
        try {
            if (!canStartLoad(connControl, 6, task.getId())) {
                System.out.println("Không đủ điều kiện để bắt đầu load hôm nay. Thoát chương trình.");
                mailService.sendMailTo(
                        "22130217@st.hcmuaf.edu.vn",
                        "[DW][INFO]  Không đủ điều kiện để Load",
                        "Không đủ điều kiện để bắt đầu load cho job id = " + task.getId() +
                                ". Quy trình sẽ dừng. Vui lòng kiểm tra đủ điều kiện để mới có thể thực hiện load."
                );
                System.exit(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    // ======================== 2.1.5 Bắt đầu ghi log vào process_log, trạng thái PROCESSING ==========================
    private void startProcessLog() {
        try {
            logId = logStart(connControl, task.getId(), jobName, "Bắt đầu kết nối và load...");
            conn.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ======================== 2.1.6 Gọi hàm createTableIfNotExists ==========================
    private void createTableIfNotExistsWrapper() {
        try {
            String ddl = """
                    CREATE TABLE IF NOT EXISTS stg_nintendo_games_raw (
                        `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                        `name` VARCHAR(512) NOT NULL,
                        `price` VARCHAR(32),
                        `size` VARCHAR(128),
                        `support` VARCHAR(256),
                        `of_players` VARCHAR(256),
                        `genre` VARCHAR(256),
                        `system` VARCHAR(128),
                        `publisher` VARCHAR(128),
                        `developer` VARCHAR(128),
                        `languages` TEXT,
                        `release_date` VARCHAR(64),
                        `source` VARCHAR(32) NOT NULL DEFAULT 'nintendo',
                        `file_name` VARCHAR(255) NOT NULL,
                        `load_ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """;
            try (Statement st = conn.createStatement()) {
                st.execute(ddl);
            }
        } catch (Exception e) {
            handleTableError(e, "create");
        }
    }

    // ======================== 2.1.7 Gọi hàm truncateTable ==========================
    private void truncateTableWrapper() {
        try {
            truncateTable(conn);
        } catch (Exception e) {
            handleTableError(e, "truncate");
        }
    }

    // ======================== 2.1.8 Gọi hàm loadCsvIntoTable ==========================
    private void loadCsvIntoTableWrapper() throws SQLException {
        try {
            String source = "nintendo";
            String insertSql = """
                    INSERT INTO stg_nintendo_games_raw (
                        `name`, `price`, `size`, `support`, `of_players`, `genre`,
                        `system`, `publisher`, `developer`, `languages`, `release_date`,
                        `source`, `file_name`, `load_ts`
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, NOW())
                    """;
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                int inserted = loadCsvIntoTable(ps, Paths.get(csvFilePath), source);
                logEnd(connControl, logId, "DONE", "Load thành công. Total records: " + inserted);
                conn.commit();
                System.out.printf("Inserted total: %d records.%n", inserted);
            }
        } catch (Exception e) {
            // 2.6.9 In lỗi khi insert thất bại vào table stg_nintendo_games_raw
             e.printStackTrace();

            // 2.6.10 Ghi log lại process là status lỗi
            logEnd(connControl, logId, "ERROR", "Lỗi DB (connect/create/truncate): " + e.getMessage());
            // 2.6.11 Thực hiện gửi mail thông báo lỗi khi kết nối DB

            mailService.sendMailTo(
                    "22130217@st.hcm    uaf.edu.vn",
                    "[DW][ERROR] Lỗi Insert vào stg_nintendo_games_raw",
                    "Lỗi khi insert dữ liệu vào bảng stg_nintendo_games_raw cho task id = " + task.getId() +
                            ". Lỗi DB: " + e.getMessage()
            );
        }
    }

    // ======================== 2.1.9  finish ==========================
    private void finish() {
        try {
            if (conn != null) conn.close();
            if (connControl != null) connControl.close();
            System.out.println("Kết thúc quá trình Load Nintendo.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ======================== Helper & DB functions ==========================
    private void handleTableError(Exception e, String action) {
        e.printStackTrace();
        try {
            logEnd(connControl, logId, "ERROR", "Lỗi DB khi " + action + " table: " + e.getMessage());
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        mailService.sendMailTo(
                "22130217@st.hcmuaf.edu.vn",
                "[DW][ERROR] Lỗi " + action + " table",
                "Lỗi DB cho task id = " + task.getId() + ". Lỗi: " + e.getMessage()
        );
        System.exit(1);
    }



    private static void truncateTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("TRUNCATE TABLE stg_nintendo_games_raw");
        }
    }

    private static int loadCsvIntoTable(PreparedStatement ps, Path p, String source) throws Exception {
        int count = 0, pending = 0;
        Reader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreEmptyLines().withTrim().parse(reader);

        for (CSVRecord rec : parser) {
            int i = 1;
            ps.setString(i++, get(rec, "name"));
            ps.setString(i++, get(rec, "price"));
            ps.setString(i++, get(rec, "size"));
            ps.setString(i++, get(rec, "support"));
            ps.setString(i++, get(rec, "of_players"));
            ps.setString(i++, get(rec, "genre"));
            ps.setString(i++, get(rec, "system"));
            ps.setString(i++, get(rec, "publisher"));
            ps.setString(i++, get(rec, "developer"));
            ps.setString(i++, get(rec, "languages"));
            ps.setString(i++, get(rec, "release_date"));
            ps.setString(i++, source);
            ps.setString(i++, p.getFileName().toString());

            ps.addBatch();
            pending++;
            if (pending >= BATCH_SIZE) {
                count += exec(ps);
                pending = 0;
            }
        }
        if (pending > 0) count += exec(ps);
        return count;
    }

    private static int exec(PreparedStatement ps) throws SQLException {
        int sum = 0;
        for (int r : ps.executeBatch()) sum += Math.max(r, 0);
        return sum;
    }

    private static String get(CSVRecord rec, String name) {
        try {
            String v = rec.isMapped(name) ? rec.get(name) : "";
            return (v == null) ? "" : v;
        } catch (Exception e) {
            return "";
        }
    }

    private static boolean canStartLoad(Connection connControl, int idJobConfigExtract, int idJobConfigLoad) throws SQLException {
        String today = LocalDate.now().toString();

        String sqlExtract = "SELECT COUNT(*) FROM process_log WHERE job_id = ? AND status = 'DONE' AND DATE(created_at) = ?";
        try (PreparedStatement ps = connControl.prepareStatement(sqlExtract)) {
            ps.setInt(1, idJobConfigExtract);
            ps.setString(2, today);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getInt(1) == 0) {
                    System.out.println("Chưa có extract DONE hôm nay.");
                    return false;
                }
            }
        }

        String sqlLoad = "SELECT COUNT(*) FROM process_log WHERE job_id = ? AND status = 'PROCESSING' AND DATE(created_at) = ?";
        try (PreparedStatement ps = connControl.prepareStatement(sqlLoad)) {
            ps.setInt(1, idJobConfigLoad);
            ps.setString(2, today);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getInt(1) > 0) {
                    System.out.println("Đang có process load khác đang PROCESSING.");
                    return false;
                }
            }
        }

        return true;
    }

    private static long logStart(Connection conn, int jobId, String name, String description) throws SQLException {
        String sql = "INSERT INTO process_log (job_id, name, description, status, created_at, updated_at) VALUES (?, ?, ?, 'PROCESSING', NOW(), NOW())";
        long logId = -1;
        conn.setAutoCommit(true);
        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setInt(1, jobId);
            ps.setString(2, name);
            ps.setString(3, description);
            int affectedRows = ps.executeUpdate();
            if (affectedRows > 0) try (ResultSet rs = ps.getGeneratedKeys()) { if (rs.next()) logId = rs.getLong(1); }
        }
        System.out.println("Bắt đầu log (ID: " + logId + ") cho Job " + jobId);
        return logId;
    }

    private static void logEnd(Connection conn, long logId, String status, String description) throws SQLException {
        String sql = "UPDATE process_log SET status = ?, description = ?, updated_at = NOW() WHERE id = ?";
        conn.setAutoCommit(true);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setString(2, description);
            ps.setLong(3, logId);
            ps.executeUpdate();
        }
        System.out.println("Kết thúc log (ID: " + logId + "), Status: " + status);
    }

}
