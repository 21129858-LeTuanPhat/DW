package vn.edu.hcmuaf.fit;

import com.fasterxml.jackson.databind.ObjectMapper;
import vn.edu.hcmuaf.fit.config.JsonConfig;
import vn.edu.hcmuaf.fit.config.Task;
import vn.edu.hcmuaf.fit.email.EmailHelper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class NintendoGamesDataNormalizer {

    // Input params & process objects
    private Task task;
    private JsonConfig jsonConfig;
    private String jobName;

    // DB connections & log
    private Connection conn;
    private long processId = 0;
    private Process psLoad;

    public static void main(String[] args) {
        NintendoGamesDataNormalizer process = new NintendoGamesDataNormalizer();

        // 6.1.0 Thực thi process để load config
        process.executeLoadConfig();

        // 6.1.1 Parse JsonConfig để kết nối database control
        process.parseJsonConfig();

        // 6.1.2 Set config cho DbProcess
        process.setupDbProcess();

        // 6.1.3 Kiểm tra điều kiện cho phép thực hiện Normalize
        process.checkNormalizeCondition();

        // 6.1.4 Đọc kết quả JSON trả về từ process LoadConfig
        process.readTaskFromProcess();

        // 6.1.5 Insert log vào process_log với status PROCESSING
        process.startProcessLog();

        // 6.1.6 Xác định thông tin và mở kết nối database warehouse
        process.openConnection();

        // 6.1.7 Gọi hàm createNormalizedTablesWrapper() để tạo các bảng chuẩn hóa
        process.createNormalizedTablesWrapper();

        // 6.1.8 Load file CSV vào bảng nintendo_games
        process.loadCsvFileWrapper();

        // 6.1.9 Gọi hàm normalizeAndLoadWrapper() để chuẩn hóa và load dữ liệu vào các bảng normalized
        process.normalizeAndLoadWrapper();

        // 6.1.10 In thống kê dữ liệu của data warehouse sau khi normalize
        process.printStatisticsWrapper();

        // 6.1.11 Cập nhật process_log thành DONE và đóng kết nối
        process.finish();
    }

    // ======================== 6.1.0 Thực thi process để load config ==========================
    private void executeLoadConfig() {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    "java", "-jar",
                    "D:\\DW\\file_jar\\LoadConfig1-1.0-SNAPSHOT.jar",
                    "10", "3", "D:\\DW\\configs.json"
            );
            pb.redirectErrorStream(true);
            this.psLoad = pb.start();

        } catch (Exception e) {
            // 6.2.1 In ra lỗi khi thực thi LoadConfig
            e.printStackTrace();
            // 6.2.2 Gửi mail thông báo lỗi thực thi LoadConfig
            EmailHelper.Send(
                    "Lỗi Job Nintendo Normalize",
                    "Không thể chạy LoadConfig",
                    "<h1 style=\"color: red;\">Error!!!</h1><p>Không thể thực thi LoadConfig.</p>"
            );
            System.exit(1);
        }
    }

    // ======================== 6.1.1 Parse JsonConfig để kết nối database control ==========================
    private void parseJsonConfig() {
        try {
            this.jsonConfig = DbProcess.getConfigJson("D:\\DW\\configs.json");

        } catch (Exception e) {
            // 6.3.1 In ra lỗi khi Parse JsonConfig
            e.printStackTrace();
            // 6.3.2 Gửi mail thông báo lỗi Parse JsonConfig
            EmailHelper.Send(
                    "Lỗi Job Nintendo Normalize",
                    "Lỗi khi Parse JsonConfig",
                    "<h1 style=\"color: red;\">Error!!!</h1><p>" + e.getMessage() + "</p>"
            );
            System.exit(1);
        }
    }

    // ======================== 6.1.2 Set config cho DbProcess ==========================
    private void setupDbProcess() {
        DbProcess.setConfig(jsonConfig);
    }

    // ======================== 6.1.3 Kiểm tra điều kiện cho phép thực hiện Normalize ==========================
    private void checkNormalizeCondition() {
        if (DbProcess.checkHasProcessAndNotError()) {
            // 6.4.1 In ra thông báo không đủ điều kiện
            System.out.println("Không thể thực hiện tiến trình do bị tiến trình khác block");
            // 6.4.2 Gửi mail thông báo không đủ điều kiện
            EmailHelper.Send(
                    "Lỗi Job Nintendo Normalize",
                    "Không thể thực hiện tiến trình do bị tiến trình khác block",
                    "<h1 style=\"color: red;\">Error!!!</h1>"
            );
            System.exit(0);
        }
    }

    // ======================== 6.1.4 Đọc kết quả JSON trả về từ process LoadConfig ==========================
    private void readTaskFromProcess() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(psLoad.getInputStream()))) {

            String line;
            StringBuilder output = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }

            int exitCode = psLoad.waitFor();
            System.out.println("Exit code: " + exitCode);

            ObjectMapper mapper = new ObjectMapper();
            this.task = mapper.readValue(output.toString(), Task.class);
            this.jobName = "Normalize Nintendo (Job " + task.getId() + ")";

        } catch (Exception e) {
            // 6.5.1 In ra lỗi khi đọc/parse JSON
            e.printStackTrace();
            // 6.5.2 Gửi mail thông báo lỗi đọc/parse JSON
            EmailHelper.Send(
                    "Lỗi Job Nintendo Normalize",
                    "Lỗi khi đọc output từ LoadConfig JAR",
                    "<h1 style=\"color: red;\">Error!!!</h1><p>" + e.getMessage() + "</p>"
            );
            System.exit(1);
        }
    }

    // ======================== 6.1.5 Insert log vào process_log với status PROCESSING ==========================
    private void startProcessLog() {
        try {
            this.processId = DbProcess.insertLog(
                    task.getName(),
                    "PROCESSING",
                    task.getDescription(),
                    task.getId()
            );
            System.out.println("Bắt đầu log (ID: " + processId + ") cho Job " + task.getId());

        } catch (Exception e) {
            // 6.6.1 In ra lỗi khi insert log
            e.printStackTrace();
            // 6.6.2 Gửi mail thông báo lỗi insert log
            EmailHelper.Send(
                    "Lỗi Job Nintendo Normalize",
                    "Lỗi khi insert Process Log",
                    "<h1 style=\"color: red;\">Error!!!</h1><p>" + e.getMessage() + "</p>"
            );
            System.exit(1);
        }
    }

    // ======================== 6.1.6 Mở kết nối đến database warehouse ==========================
    private void openConnection() {
        try {
            String serverIp = task.getDbConfig().getServerIp();
            int port = task.getDbConfig().getPort();
            String dbName = task.getDbConfig().getName();
            String username = task.getDbConfig().getUser();
            String password = task.getDbConfig().getPassword();
            String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s", serverIp, port, dbName);

            this.conn = DriverManager.getConnection(jdbcUrl, username, password);

        } catch (Exception e) {
            e.printStackTrace();
            handleError(e, "Lỗi kết nối database warehouse");
        }
    }

    // ======================== 6.1.7 Gọi hàm createNormalizedTablesWrapper() để tạo các bảng chuẩn hóa ==========================
    private void createNormalizedTablesWrapper() {
        try {
            System.out.println("=".repeat(60));
            System.out.println("Nintendo Games Data Normalizer (Using Stored Procedures)");
            System.out.println("=".repeat(60));

            createNormalizedTables(conn);

        } catch (Exception e) {
            handleError(e, "Lỗi tạo normalized tables");
        }
    }

    // ======================== 6.1.8 Load file CSV vào bảng nintendo_games ==========================
    private void loadCsvFileWrapper() {
        try {
            loadCsvFile(conn, task.getSourcePath());

        } catch (Exception e) {
            handleError(e, "Lỗi load CSV file");
        }
    }

    // ======================== 6.1.9 Gọi hàm normalizeAndLoadWrapper() để chuẩn hóa và load dữ liệu vào các bảng normalized ==========================
    private void normalizeAndLoadWrapper() {
        try {
            normalizeAndLoad(conn, processId);

        } catch (Exception e) {
            // 6.7.1 In ra lỗi khi normalize
            e.printStackTrace();
            // 6.7.2 Cập nhật process_log thành ERROR
            DbProcess.updateLog(processId, "ERROR");
            // 6.7.3 Gửi mail thông báo lỗi Normalize
            EmailHelper.Send(
                    "Lỗi Job Nintendo Normalize",
                    "Lỗi khi Normalize dữ liệu",
                    "<h1 style=\"color: red;\">Error!!!</h1><p>" + e.getMessage() + "</p>"
            );
            System.exit(1);
        }
    }

    // ======================== 6.1.10 In thống kê dữ liệu của data warehouse sau khi normalize ==========================
    private void printStatisticsWrapper() {
        try {
            printStatistics(conn);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ======================== 6.1.11 Cập nhật process_log thành DONE và đóng kết nối ==========================
    private void finish() {
        try {
            if (conn != null) conn.close();
            DbProcess.updateLog(processId, "DONE");
            System.out.println("Kết thúc log (ID: " + processId + "), Status: DONE");
            System.out.println("Kết thúc quá trình Normalize Nintendo.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ======================== Helper & Error Handling ==========================
    private void handleError(Exception e, String message) {
        e.printStackTrace();
        try {
            if (processId > 0) {
                DbProcess.updateLog(processId, "ERROR");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        EmailHelper.Send(
                "Lỗi Job Nintendo Normalize",
                message,
                "<h1 style=\"color: red;\">Error!!!</h1><p>" + e.getMessage() + "</p>"
        );
        System.exit(1);
    }

    // ======================== Database Functions ==========================

    /**
     * Tạo các dimension tables và fact table bằng stored procedure
     */
    private static void createNormalizedTables(Connection conn) throws SQLException {
        System.out.println("Creating normalized tables...");
        try (CallableStatement cstmt = conn.prepareCall("{CALL sp_create_normalized_tables()}")) {
            cstmt.execute();

            try (ResultSet rs = cstmt.getResultSet()) {
                if (rs != null && rs.next()) {
                    System.out.println("✓ " + rs.getString("message"));
                }
            }
        }
    }

    /**
     * Xóa toàn bộ dữ liệu trong các bảng normalized bằng stored procedure
     */
    private static void truncateAllNormalizedTables(Connection conn) throws SQLException {
        System.out.println("\nTruncating all normalized tables...");
        try (CallableStatement cstmt = conn.prepareCall("{CALL sp_truncate_normalized_tables()}")) {
            cstmt.execute();

            try (ResultSet rs = cstmt.getResultSet()) {
                if (rs != null && rs.next()) {
                    System.out.println("✓ " + rs.getString("message"));
                }
            }
        }
    }

    /**
     * Insert hoặc get ID của genre bằng stored procedure
     */
    private static int getOrInsertGenreId(Connection conn, String genreName) throws SQLException {
        try (CallableStatement cstmt = conn.prepareCall("{CALL sp_get_or_insert_genre(?, ?)}")) {
            cstmt.setString(1, genreName.trim());
            cstmt.registerOutParameter(2, Types.INTEGER);
            cstmt.execute();
            return cstmt.getInt(2);
        }
    }

    /**
     * Insert hoặc get ID của language bằng stored procedure
     */
    private static int getOrInsertLanguageId(Connection conn, String languageName) throws SQLException {
        try (CallableStatement cstmt = conn.prepareCall("{CALL sp_get_or_insert_language(?, ?)}")) {
            cstmt.setString(1, languageName.trim());
            cstmt.registerOutParameter(2, Types.INTEGER);
            cstmt.execute();
            return cstmt.getInt(2);
        }
    }

    /**
     * Insert hoặc get ID của publisher bằng stored procedure
     */
    private static Integer getOrInsertPublisherId(Connection conn, String publisherName) throws SQLException {
        if (publisherName == null || publisherName.trim().isEmpty()) {
            return null;
        }
        try (CallableStatement cstmt = conn.prepareCall("{CALL sp_get_or_insert_publisher(?, ?)}")) {
            cstmt.setString(1, publisherName.trim());
            cstmt.registerOutParameter(2, Types.INTEGER);
            cstmt.execute();

            int id = cstmt.getInt(2);
            return cstmt.wasNull() ? null : id;
        }
    }

    /**
     * Insert hoặc get ID của developer bằng stored procedure
     */
    private static Integer getOrInsertDeveloperId(Connection conn, String developerName) throws SQLException {
        if (developerName == null || developerName.trim().isEmpty()) {
            return null;
        }
        try (CallableStatement cstmt = conn.prepareCall("{CALL sp_get_or_insert_developer(?, ?)}")) {
            cstmt.setString(1, developerName.trim());
            cstmt.registerOutParameter(2, Types.INTEGER);
            cstmt.execute();

            int id = cstmt.getInt(2);
            return cstmt.wasNull() ? null : id;
        }
    }

    /**
     * Insert hoặc get ID của system bằng stored procedure
     */
    private static Integer getOrInsertSystemId(Connection conn, String systemName) throws SQLException {
        if (systemName == null || systemName.trim().isEmpty()) {
            return null;
        }
        try (CallableStatement cstmt = conn.prepareCall("{CALL sp_get_or_insert_system(?, ?)}")) {
            cstmt.setString(1, systemName.trim());
            cstmt.registerOutParameter(2, Types.INTEGER);
            cstmt.execute();

            int id = cstmt.getInt(2);
            return cstmt.wasNull() ? null : id;
        }
    }

    /**
     * Insert game bằng stored procedure
     */
    private static long insertGame(Connection conn, String name, String priceUsd, String size, String support,
                                   String ofPlayers, Integer systemId, Integer publisherId,
                                   Integer developerId, String releaseDate, String source,
                                   String fileName, int datesk, String loadExpired,
                                   String loadStaging, int isDeleted) throws SQLException {
        try (CallableStatement cstmt = conn.prepareCall(
                "{CALL sp_insert_game(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?)}")) {

            cstmt.setString(1, name);
            cstmt.setString(2, priceUsd);
            cstmt.setString(3, size);
            cstmt.setString(4, support);
            cstmt.setString(5, ofPlayers);

            if (systemId != null) {
                cstmt.setInt(6, systemId);
            } else {
                cstmt.setNull(6, Types.INTEGER);
            }

            if (publisherId != null) {
                cstmt.setInt(7, publisherId);
            } else {
                cstmt.setNull(7, Types.INTEGER);
            }

            if (developerId != null) {
                cstmt.setInt(8, developerId);
            } else {
                cstmt.setNull(8, Types.INTEGER);
            }

            cstmt.setString(9, releaseDate);
            cstmt.setString(10, source);
            cstmt.setString(11, fileName);
            cstmt.setInt(12, datesk);
            cstmt.setString(13, loadExpired);
            cstmt.setString(14, loadStaging);
            cstmt.setInt(15, isDeleted);
            cstmt.registerOutParameter(16, Types.BIGINT);

            cstmt.execute();

            return cstmt.getLong(16);
        }
    }

    /**
     * Insert game-genre relationship bằng stored procedure
     */
    private static void insertGameGenre(Connection conn, long gameKey, int genreId) throws SQLException {
        try (CallableStatement cstmt = conn.prepareCall("{CALL sp_insert_game_genre(?, ?)}")) {
            cstmt.setLong(1, gameKey);
            cstmt.setInt(2, genreId);
            cstmt.execute();
        }
    }

    /**
     * Insert game-language relationship bằng stored procedure
     */
    private static void insertGameLanguage(Connection conn, long gameKey, int languageId) throws SQLException {
        try (CallableStatement cstmt = conn.prepareCall("{CALL sp_insert_game_language(?, ?)}")) {
            cstmt.setLong(1, gameKey);
            cstmt.setInt(2, languageId);
            cstmt.execute();
        }
    }

    /**
     * Tách string thành list dựa trên delimiter
     */
    private static List<String> splitAndTrim(String value, String delimiter) {
        List<String> result = new ArrayList<>();
        if (value == null || value.trim().isEmpty() || value.equalsIgnoreCase("Chưa có")) {
            return result;
        }
        String[] parts = value.split(delimiter);
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty() && !trimmed.equalsIgnoreCase("Chưa có")) {
                result.add(trimmed);
            }
        }
        return result;
    }

    /**
     * 6.1.10 Normalize và load data từ nintendo_games table
     */
    private static void normalizeAndLoad(Connection conn, long processId) throws SQLException {
        try {
            conn.setAutoCommit(false);

            truncateAllNormalizedTables(conn);

            System.out.println("\nNormalizing data from nintendo_games...");

            String selectSql = "SELECT * FROM nintendo_games ORDER BY id";

            int gameCount = 0;
            int genreCount = 0;
            int languageCount = 0;

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(selectSql)) {

                while (rs.next()) {
                    try {
                        Integer systemId = getOrInsertSystemId(conn, rs.getString("system"));
                        Integer publisherId = getOrInsertPublisherId(conn, rs.getString("publisher"));
                        Integer developerId = getOrInsertDeveloperId(conn, rs.getString("developer"));

                        long gameKey = insertGame(
                                conn,
                                rs.getString("name"),
                                rs.getString("price_usd"),
                                rs.getString("size"),
                                rs.getString("support"),
                                rs.getString("of_players"),
                                systemId,
                                publisherId,
                                developerId,
                                rs.getString("release_date"),
                                rs.getString("source"),
                                rs.getString("file_name"),
                                rs.getInt("date_sk"),
                                rs.getString("load_expired"),
                                rs.getString("load_staging"),
                                rs.getInt("is_deleted")
                        );
                        gameCount++;

                        String genresStr = rs.getString("genre");
                        List<String> genres = splitAndTrim(genresStr, ",");
                        for (String genre : genres) {
                            int genreId = getOrInsertGenreId(conn, genre);
                            insertGameGenre(conn, gameKey, genreId);
                            genreCount++;
                        }

                        String languagesStr = rs.getString("languages");
                        List<String> languages = splitAndTrim(languagesStr, ",");
                        for (String language : languages) {
                            int languageId = getOrInsertLanguageId(conn, language);
                            insertGameLanguage(conn, gameKey, languageId);
                            languageCount++;
                        }

                    } catch (Exception e) {
                        // 6.9.1 In ra lỗi khi xử lý game (tiếp tục xử lý)
                        System.err.println("Error processing game: " + rs.getString("name"));
                        e.printStackTrace();
                    }
                }
            }

            conn.commit();

            System.out.println("\n" + "=".repeat(60));
            System.out.println("✓ Normalization Complete");
            System.out.println("=".repeat(60));
            System.out.println("Games loaded:                  " + gameCount);
            System.out.println("Game-Genre relationships:      " + genreCount);
            System.out.println("Game-Language relationships:   " + languageCount);
            System.out.println("=".repeat(60));

        } catch (Exception e) {
            // 6.9.1 Rollback transaction khi có lỗi
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    /**
     * 6.1.9 Load file CSV vào bảng nintendo_games
     */
    private static void loadCsvFile(Connection conn, String csvPath) throws SQLException {
        String tableName = "nintendo_games";
        String[] columns = {
                "id", "name", "price_usd", "size", "support", "of_players",
                "genre", "system", "publisher", "developer", "languages",
                "release_date", "release_year", "release_month", "source",
                "file_name", "load_ts", "load_expired", "load_staging",
                "is_deleted", "date_sk"
        };

        try (PreparedStatement pstmt = conn.prepareStatement("TRUNCATE TABLE " + tableName)) {
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        String colNames = Arrays.stream(columns)
                .map(col -> "`" + col + "`")
                .collect(Collectors.joining(","));
        String placeholders = String.join(",", Collections.nCopies(columns.length, "?"));
        String sql = "INSERT INTO " + tableName + " (" + colNames + ") VALUES (" + placeholders + ")";

        try (BufferedReader br = new BufferedReader(new FileReader(csvPath));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);

            // Bỏ qua dòng header
            br.readLine();

            String line;
            int count = 0;

            while ((line = br.readLine()) != null) {
                String[] data = line.split(";", -1);

                pstmt.setLong(1, Long.parseLong(data[0])); // id
                pstmt.setString(2, data[1]); // name

                // price_usd - xử lý null
                if (data[2] == null || data[2].trim().isEmpty()) {
                    pstmt.setNull(3, Types.DECIMAL);
                } else {
                    pstmt.setBigDecimal(3, new BigDecimal(data[2].trim()));
                }

                pstmt.setString(4, data[3]); // size
                pstmt.setString(5, data[4]); // support
                pstmt.setString(6, data[5]); // of_players
                pstmt.setString(7, data[6]); // genre
                pstmt.setString(8, data[7]); // system
                pstmt.setString(9, data[8]); // publisher
                pstmt.setString(10, data[9]); // developer
                pstmt.setString(11, data[10]); // languages

                // release_date - xử lý null
                if (data[11].isEmpty()) {
                    pstmt.setNull(12, Types.DATE);
                } else {
                    pstmt.setDate(12, Date.valueOf(data[11]));
                }

                // release_year - xử lý null
                if (data[12].isEmpty()) {
                    pstmt.setNull(13, Types.INTEGER);
                } else {
                    pstmt.setInt(13, Integer.parseInt(data[12]));
                }

                // release_month - xử lý null
                if (data[13].isEmpty()) {
                    pstmt.setNull(14, Types.INTEGER);
                } else {
                    pstmt.setInt(14, Integer.parseInt(data[13]));
                }

                pstmt.setString(15, data[14]); // source
                pstmt.setString(16, data[15]); // file_name
                pstmt.setTimestamp(17, Timestamp.valueOf(data[16])); // load_ts
                pstmt.setTimestamp(18, Timestamp.valueOf(data[17])); // load_expired
                pstmt.setTimestamp(19, Timestamp.valueOf(data[18])); // load_staging
                pstmt.setInt(20, Integer.parseInt(data[19])); // is_deleted
                pstmt.setInt(21, Integer.parseInt(data[20])); // date_sk

                pstmt.executeUpdate();
                count++;
            }

            conn.commit();
            System.out.println("Import thành công " + count + " bản ghi!");

        } catch (Exception e) {
            conn.rollback();
            throw new SQLException("Lỗi khi load CSV file", e);
        } finally {
            conn.setAutoCommit(true);
        }
    }

    /**
     * 6.1.11 In thống kê dữ liệu đã normalize bằng stored procedure
     */
    private static void printStatistics(Connection conn) throws SQLException {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("Database Statistics");
        System.out.println("=".repeat(60));

        try (CallableStatement cstmt = conn.prepareCall("{CALL sp_get_statistics()}")) {
            cstmt.execute();

            try (ResultSet rs = cstmt.getResultSet()) {
                while (rs != null && rs.next()) {
                    System.out.printf("%-30s: %,d%n",
                            rs.getString("table_name"),
                            rs.getInt("count"));
                }
            }
        }

        System.out.println("=".repeat(60));
    }
}