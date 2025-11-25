package vn.edu.hcmuaf.fit;

import com.fasterxml.jackson.databind.ObjectMapper;
import vn.edu.hcmuaf.fit.config.JsonConfig;
import vn.edu.hcmuaf.fit.config.Task;
import vn.edu.hcmuaf.fit.email.EmailHelper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NintendoGamesDataTransformer {

    // Input params & process objects
    private Task task;
    private JsonConfig jsonConfig;
    private String jobName;

    // DB connections & log
    private Connection conn;
    private long processId = 0;
    private Process psLoad;

    public static void main(String[] args) {
        NintendoGamesDataTransformer process = new NintendoGamesDataTransformer();

        // 3.1.0 Thực thi process để load config
        process.executeLoadConfig();

        // 3.1.1 Parse JsonConfig để kết nối database control
        process.parseJsonConfig();

        // 3.1.2 Set config cho DbProcess
        process.setupDbProcess();

        // 3.1.3 Kiểm tra điều kiện cho phép thực hiện Transform
        process.checkTransformCondition();

        // 3.1.4 Đọc kết quả JSON trả về từ process LoadConfig
        process.readTaskFromProcess();

        // 3.1.5 Insert log vào process_log với status PROCESSING
        process.startProcessLog();

        // 3.1.6 Xác định thông tin và mở kết nối database staging
        process.openConnection();

        // 3.1.7 Gọi hàm createTargetTableIfNotExistsWrapper() để kiểm tra và tạo bảng nếu chưa tồn tại
        process.createTargetTableIfNotExistsWrapper();

        // 3.1.8 Gọi hàm truncateTargetTableWrapper() để làm sạch bảng trước khi load dữ liệu mới
        process.truncateTargetTableWrapper();

        // 3.1.9 Gọi hàm transformAndLoadWrapper() để thực hiện Transform & Load dữ liệu
        process.transformAndLoadWrapper();


        // 3.1.10 Cập nhật process_log thành DONE và đóng kết nối
        process.finish();
    }

    // ======================== 3.1.0 Thực thi process để load config ==========================
    private void executeLoadConfig() {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    "java", "-jar",
                    "D:\\DW\\file_jar\\LoadConfig1-1.0-SNAPSHOT.jar",
                    "8", "2", "D:\\DW\\configs.json"
            );
            pb.redirectErrorStream(true);
            this.psLoad = pb.start();

        } catch (Exception e) {
            // 3.2.1 In ra lỗi load config thất bại
            e.printStackTrace();
            // 3.2.2 Gửi mail thông báo lỗi thực thi LoadConfig đến Admin
            EmailHelper.Send(
                    "Lỗi Job Nintendo Transform",
                    "Không thể chạy LoadConfig",
                    "<h1 style=\"color: red;\">Error!!!</h1><p>Không thể thực thi LoadConfig.</p>"
            );
            System.exit(1);
        }
    }

    // ======================== 3.1.1 Parse JsonConfig để kết nối database control ==========================
    private void parseJsonConfig() {
        try {
            this.jsonConfig = DbProcess.getConfigJson("D:\\DW\\configs.json");

        } catch (Exception e) {
            // 3.3.1 In ra lỗi khi Parse JsonConfig
            e.printStackTrace();
            // 3.3.2 Gửi mail thông báo lỗi Parse JsonConfig
            EmailHelper.Send(
                    "Lỗi Job Nintendo Transform",
                    "Lỗi khi Parse JsonConfig",
                    "<h1 style=\"color: red;\">Error!!!</h1><p>" + e.getMessage() + "</p>"
            );
            System.exit(1);
        }
    }

    // ======================== 3.1.2 Set config cho DbProcess ==========================
    private void setupDbProcess() {
        DbProcess.setConfig(jsonConfig);
    }

    // ======================== 3.1.3 Kiểm tra điều kiện cho phép thực hiện Transform ==========================
    private void checkTransformCondition() {
        if (DbProcess.checkHasProcessAndNotError()) {
            // 3.4.1 In ra thông báo không đủ điều kiện
            System.out.println("Không thể thực hiện tiến trình do bị tiến trình khác block");
            // 3.4.2 Gửi mail thông báo không đủ điều kiện
            EmailHelper.Send(
                    "Lỗi Job Nintendo Transform",
                    "Không thể thực hiện tiến trình do bị tiến trình khác block",
                    "<h1 style=\"color: red;\">Error!!!</h1>"
            );
            System.exit(0);
        }
    }

    // ======================== 3.1.4 Đọc kết quả JSON trả về từ process LoadConfig ==========================
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
            this.jobName = "Transform Nintendo (Job " + task.getId() + ")";

        } catch (Exception e) {
            // 3.5.1 In ra lỗi khi đọc/parse JSON
            e.printStackTrace();
            // 3.5.2 Gửi mail thông báo lỗi đọc/parse JSON
            EmailHelper.Send(
                    "Lỗi Job Nintendo Transform",
                    "Lỗi khi đọc output từ LoadConfig JAR",
                    "<h1 style=\"color: red;\">Error!!!</h1><p>" + e.getMessage() + "</p>"
            );
            System.exit(1);
        }
    }

    // ======================== 3.1.5 Insert log vào process_log với status PROCESSING ==========================
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
            // 3.6.1 In ra lỗi khi insert log
            e.printStackTrace();
            // 3.6.2 Gửi mail thông báo lỗi insert log
            EmailHelper.Send(
                    "Lỗi Job Nintendo Transform",
                    "Lỗi khi insert Process Log",
                    "<h1 style=\"color: red;\">Error!!!</h1><p>" + e.getMessage() + "</p>"
            );
            System.exit(1);
        }
    }

    // ======================== 3.1.6 Mở kết nối đến database staging ==========================
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
            handleError(e, "Lỗi kết nối database staging");
        }
    }

    // ======================== 3.1.7 Gọi hàm createTargetTableIfNotExistsWrapper() để kiểm tra và tạo bảng nếu chưa tồn tại ==========================
    private void createTargetTableIfNotExistsWrapper() {
        try {
            createTargetTableIfNotExists(conn, "nintendo_games_temp");
        } catch (Exception e) {
            handleTableError(e, "create table");
        }
    }

    // ======================== 3.1.8 Gọi hàm truncateTargetTableWrapper() để làm sạch bảng trước khi load dữ liệu mới ==========================
    private void truncateTargetTableWrapper() {
        try {
            truncateTargetTable(conn, "nintendo_games_temp");
        } catch (Exception e) {
            handleTableError(e, "truncate table");
        }
    }

    // ======================== 3.1.9 Gọi hàm transformAndLoadWrapper() để thực hiện Transform & Load dữ liệu  ==========================
    private void transformAndLoadWrapper() {
        try {
            System.out.println("=".repeat(60));
            System.out.println("Nintendo Games Data Transformer");
            System.out.println("=".repeat(60));

            transformAndLoad(conn, "nintendo_games_temp", processId);

        } catch (Exception e) {
            // 3.7.1 In ra lỗi khi transform
            e.printStackTrace();
            // 3.7.2 Cập nhật process_log thành ERROR
            DbProcess.updateLog(processId, "ERROR");
            // 3.7.3 Gửi mail thông báo lỗi Transform
            EmailHelper.Send(
                    "Lỗi Job Nintendo Transform",
                    "Lỗi khi Transform dữ liệu",
                    "<h1 style=\"color: red;\">Error!!!</h1><p>" + e.getMessage() + "</p>"
            );
            System.exit(1);
        }
    }

    // ======================== 3.1.10 Cập nhật process_log thành DONE và đóng kết nối ==========================
    private void finish() {
        try {
            if (conn != null) conn.close();
            DbProcess.updateLog(processId, "DONE");
            System.out.println("Kết thúc log (ID: " + processId + "), Status: DONE");
            System.out.println("Kết thúc quá trình Transform Nintendo.");
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
                "Lỗi Job Nintendo Transform",
                message,
                "<h1 style=\"color: red;\">Error!!!</h1><p>" + e.getMessage() + "</p>"
        );
        System.exit(1);
    }

    private void handleTableError(Exception e, String action) {
        e.printStackTrace();
        try {
            DbProcess.updateLog(processId, "ERROR");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        EmailHelper.Send(
                "Lỗi Job Nintendo Transform",
                "Lỗi " + action,
                "<h1 style=\"color: red;\">Error!!!</h1><p>" + e.getMessage() + "</p>"
        );
        System.exit(1);
    }

    // ======================== Inner Class: NintendoGame ==========================
    public static class NintendoGame {
        private String name;
        private BigDecimal priceUsd;
        private String size;
        private String support;
        private String ofPlayers;
        private String genre;
        private String system;
        private String publisher;
        private String developer;
        private String languages;
        private LocalDate releaseDate;
        private Integer releaseYear;
        private Integer releaseMonth;
        private String source;
        private String fileName;
        private Timestamp loadTs;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public BigDecimal getPriceUsd() { return priceUsd; }
        public void setPriceUsd(BigDecimal priceUsd) { this.priceUsd = priceUsd; }

        public String getSize() { return size; }
        public void setSize(String size) { this.size = size; }

        public String getSupport() { return support; }
        public void setSupport(String support) { this.support = support; }

        public String getOfPlayers() { return ofPlayers; }
        public void setOfPlayers(String ofPlayers) { this.ofPlayers = ofPlayers; }

        public String getGenre() { return genre; }
        public void setGenre(String genre) { this.genre = genre; }

        public String getSystem() { return system; }
        public void setSystem(String system) { this.system = system; }

        public String getPublisher() { return publisher; }
        public void setPublisher(String publisher) { this.publisher = publisher; }

        public String getDeveloper() { return developer; }
        public void setDeveloper(String developer) { this.developer = developer; }

        public String getLanguages() { return languages; }
        public void setLanguages(String languages) { this.languages = languages; }

        public LocalDate getReleaseDate() { return releaseDate; }
        public void setReleaseDate(LocalDate releaseDate) { this.releaseDate = releaseDate; }

        public Integer getReleaseYear() { return releaseYear; }
        public void setReleaseYear(Integer releaseYear) { this.releaseYear = releaseYear; }

        public Integer getReleaseMonth() { return releaseMonth; }
        public void setReleaseMonth(Integer releaseMonth) { this.releaseMonth = releaseMonth; }

        public String getSource() { return source; }
        public void setSource(String source) { this.source = source; }

        public String getFileName() { return fileName; }
        public void setFileName(String fileName) { this.fileName = fileName; }

        public Timestamp getLoadTs() { return loadTs; }
        public void setLoadTs(Timestamp loadTs) { this.loadTs = loadTs; }
    }

    // ======================== Transform Functions ==========================
    private static NintendoGame transformRow(ResultSet rs) throws SQLException {
        NintendoGame game = new NintendoGame();

        game.setName(cleanString(rs.getString("name")));
        game.setSize(cleanString(rs.getString("size")));
        game.setSupport(cleanString(rs.getString("support")));
        game.setOfPlayers(cleanString(rs.getString("of_players")));
        game.setGenre(cleanString(rs.getString("genre")));
        game.setSystem(cleanString(rs.getString("system")));
        game.setPublisher(cleanString(rs.getString("publisher")));
        game.setDeveloper(cleanString(rs.getString("developer")));
        game.setLanguages(cleanString(rs.getString("languages")));
        game.setSource(cleanString(rs.getString("source")));
        game.setFileName(cleanString(rs.getString("file_name")));

        game.setPriceUsd(parsePrice(rs.getString("price")));

        String releaseDateStr = rs.getString("release_date");
        LocalDate releaseDate = parseReleaseDate(releaseDateStr);
        game.setReleaseDate(releaseDate);

        if (releaseDate != null) {
            game.setReleaseYear(releaseDate.getYear());
            game.setReleaseMonth(releaseDate.getMonthValue());
        } else {
            Integer year = extractYear(releaseDateStr);
            game.setReleaseYear(year);
            game.setReleaseMonth(null);
        }

        game.setLoadTs(rs.getTimestamp("load_ts"));

        return game;
    }

    private static String cleanString(String value) {
        if (value == null || value.trim().isEmpty() || value.trim().equalsIgnoreCase("Chưa có")) {
            return null;
        }
        return value.trim();
    }

    private static BigDecimal parsePrice(String priceStr) {
        if (priceStr == null || priceStr.trim().isEmpty() || priceStr.equalsIgnoreCase("Chưa có")) {
            return null;
        }

        try {
            String cleaned = priceStr.trim().replace("$", "").replace(",", "");
            return new BigDecimal(cleaned);
        } catch (NumberFormatException e) {
            System.err.println("Invalid price value: " + priceStr);
            return null;
        }
    }

    private static LocalDate parseReleaseDate(String releaseDateStr) {
        if (releaseDateStr == null || releaseDateStr.trim().isEmpty() ||
                releaseDateStr.equalsIgnoreCase("Chưa có")) {
            return null;
        }

        releaseDateStr = releaseDateStr.trim();

        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.ENGLISH);
            return LocalDate.parse(releaseDateStr, formatter);
        } catch (DateTimeParseException e1) {
            Pattern yearPattern = Pattern.compile("^(\\d{4})$");
            Matcher matcher = yearPattern.matcher(releaseDateStr);
            if (matcher.matches()) {
                int year = Integer.parseInt(matcher.group(1));
                return LocalDate.of(year, 1, 1);
            }

            System.err.println("Cannot parse release_date: " + releaseDateStr);
            return null;
        }
    }

    private static Integer extractYear(String releaseDateStr) {
        if (releaseDateStr == null || releaseDateStr.trim().isEmpty() ||
                releaseDateStr.equalsIgnoreCase("Chưa có")) {
            return null;
        }

        Pattern yearPattern = Pattern.compile("(\\d{4})");
        Matcher matcher = yearPattern.matcher(releaseDateStr);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }

        return null;
    }

    private static List<NintendoGame> transformAllRecords(Connection conn) throws SQLException {
        List<NintendoGame> games = new ArrayList<>();

        String sql = "SELECT * FROM stg_nintendo_games_raw " +
                "WHERE name IS NOT NULL AND name != '' AND name != 'Chưa có' " +
                "ORDER BY id";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                try {
                    NintendoGame game = transformRow(rs);
                    if (game.getName() != null && !game.getName().trim().isEmpty()) {
                        games.add(game);
                    }
                } catch (Exception e) {
                    // 3.8.1 In ra lỗi khi transform row
                    System.err.println("Error transforming row id=" + rs.getLong("id") + ": " + e.getMessage());
                    // 3.8.2 Gửi mail cảnh báo lỗi transform row (tiếp tục xử lý)
                    EmailHelper.Send(
                            "Warning: Transform Row Error",
                            "Lỗi transform row id=" + rs.getLong("id"),
                            "<p style=\"color: orange;\">Warning: " + e.getMessage() + "</p>"
                    );
                }
            }
        }

        return games;
    }

    private static int insertIntoTargetTableViaProcedure(Connection conn, List<NintendoGame> games, String targetTableName)
            throws SQLException {
        String sql = "{call sp_InsertNintendoGame(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}";

        int insertCount = 0;
        int errorCount = 0;

        try (CallableStatement cstmt = conn.prepareCall(sql)) {
            for (NintendoGame game : games) {
                try {
                    cstmt.setString(1, game.getName());

                    if (game.getPriceUsd() != null) {
                        cstmt.setBigDecimal(2, game.getPriceUsd());
                    } else {
                        cstmt.setNull(2, Types.DECIMAL);
                    }

                    cstmt.setString(3, game.getSize());
                    cstmt.setString(4, game.getSupport());
                    cstmt.setString(5, game.getOfPlayers());
                    cstmt.setString(6, game.getGenre());
                    cstmt.setString(7, game.getSystem());
                    cstmt.setString(8, game.getPublisher());
                    cstmt.setString(9, game.getDeveloper());
                    cstmt.setString(10, game.getLanguages());

                    if (game.getReleaseDate() != null) {
                        cstmt.setDate(11, Date.valueOf(game.getReleaseDate()));
                    } else {
                        cstmt.setNull(11, Types.DATE);
                    }

                    if (game.getReleaseYear() != null) {
                        cstmt.setInt(12, game.getReleaseYear());
                    } else {
                        cstmt.setNull(12, Types.INTEGER);
                    }

                    if (game.getReleaseMonth() != null) {
                        cstmt.setInt(13, game.getReleaseMonth());
                    } else {
                        cstmt.setNull(13, Types.INTEGER);
                    }

                    cstmt.setString(14, game.getSource());
                    cstmt.setString(15, game.getFileName());
                    cstmt.registerOutParameter(16, Types.BIGINT);

                    cstmt.execute();

                    long insertedId = cstmt.getLong(16);

                    if (insertedId > 0) {
                        insertCount++;
                        System.out.println("Inserted game: " + game.getName() + " with ID: " + insertedId);
                    }

                } catch (SQLException e) {
                    // 3.9.1 In ra lỗi khi insert game
                    errorCount++;
                    System.err.println("Error inserting game: " + game.getName() + " - " + e.getMessage());
                    // 3.9.2 Gửi mail cảnh báo lỗi insert (tiếp tục xử lý)
                    if (errorCount <= 5) {
                        EmailHelper.Send(
                                "Warning: Insert Game Error",
                                "Lỗi insert game: " + game.getName(),
                                "<p style=\"color: orange;\">Warning: " + e.getMessage() + "</p>"
                        );
                    }
                }
            }
        }

        if (errorCount > 0) {
            System.out.println("⚠ Warning: " + errorCount + " games failed to insert");
        }

        return insertCount;
    }

    private static void createTargetTableIfNotExists(Connection conn, String targetTableName) throws SQLException {
        String checkTableSql = "SHOW TABLES LIKE '" + targetTableName + "'";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(checkTableSql)) {

            if (!rs.next()) {
                System.out.println("Target table not found. Creating table: " + targetTableName);

                String createTableSql = "CREATE TABLE " + targetTableName + " (\n" +
                        "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                        "  name VARCHAR(512) NOT NULL,\n" +
                        "  price_usd DECIMAL(10,2),\n" +
                        "  size VARCHAR(128),\n" +
                        "  support VARCHAR(256),\n" +
                        "  of_players VARCHAR(256),\n" +
                        "  genre VARCHAR(256),\n" +
                        "  `system` VARCHAR(128),\n" +
                        "  publisher VARCHAR(128),\n" +
                        "  developer VARCHAR(128),\n" +
                        "  languages TEXT,\n" +
                        "  release_date DATE,\n" +
                        "  release_year INT,\n" +
                        "  release_month INT,\n" +
                        "  source VARCHAR(32) NOT NULL DEFAULT 'nintendo',\n" +
                        "  file_name VARCHAR(255) NOT NULL,\n" +
                        "  load_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                        "  PRIMARY KEY (id),\n" +
                        "  INDEX idx_release_date (release_date),\n" +
                        "  INDEX idx_release_year (release_year),\n" +
                        "  INDEX idx_system (`system`),\n" +
                        "  INDEX idx_publisher (publisher)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";

                stmt.execute(createTableSql);
                System.out.println("✓ Table created successfully!");
            } else {
                System.out.println("✓ Target table exists: " + targetTableName);
            }
        }
    }

    private static void truncateTargetTable(Connection conn, String targetTableName) throws SQLException {
        String truncateSql = "TRUNCATE TABLE " + targetTableName;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(truncateSql);
            System.out.println("✓ Truncated table: " + targetTableName);
        }
    }

    private static void transformAndLoad(Connection conn, String targetTableName, long processId) throws SQLException {
        try {
            conn.setAutoCommit(false);

            System.out.println("\nTransforming data from staging table...");

            List<NintendoGame> games = transformAllRecords(conn);
            System.out.println("Transformed " + games.size() + " records");

            System.out.println("\nLoading data into target table: " + targetTableName);

            int insertCount = insertIntoTargetTableViaProcedure(conn, games, targetTableName);

            conn.commit();

            System.out.println("\n" + "=".repeat(60));
            System.out.println("✓ Transform and Load Completed!");
            System.out.println("=".repeat(60));
            System.out.println("Total transformed: " + games.size());
            System.out.println("Successfully inserted: " + insertCount);
            System.out.println("=".repeat(60));

        } catch (Exception e) {
            // 3.10.1 Rollback transaction khi có lỗi
            conn.rollback();
            // 3.10.2 In ra lỗi nghiêm trọng
            e.printStackTrace();
            // 3.10.3 Gửi mail thông báo lỗi nghiêm trọng
            EmailHelper.Send(
                    "Lỗi Job Nintendo Transform",
                    "Lỗi nghiêm trọng trong quá trình Transform & Load",
                    "<h1 style=\"color: red;\">Critical Error!!!</h1><p>" + e.getMessage() + "</p>"
            );
            throw e;

        } finally {
            conn.setAutoCommit(true);
        }
    }
}