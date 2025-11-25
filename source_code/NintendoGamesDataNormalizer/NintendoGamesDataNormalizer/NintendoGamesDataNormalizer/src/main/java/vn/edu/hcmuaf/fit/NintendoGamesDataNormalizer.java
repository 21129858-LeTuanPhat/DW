package vn.edu.hcmuaf.fit;

import com.fasterxml.jackson.databind.ObjectMapper;
import vn.edu.hcmuaf.fit.config.Task;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Normalize dữ liệu Nintendo Games - sử dụng Stored Procedures
 * Version 3: Refactored to use stored procedures instead of direct SQL
 */
public class NintendoGamesDataNormalizer {
    private Connection connection;
    private long processID;

    public NintendoGamesDataNormalizer(Connection connection,  long processID) {
        this.connection = connection;
        this.processID = processID;
    }

    /**
     * Tạo các dimension tables và fact table bằng stored procedure
     */
    public void createNormalizedTables() throws SQLException {
        System.out.println("Creating normalized tables...");
        try (CallableStatement cstmt = connection.prepareCall("{CALL sp_create_normalized_tables()}")) {
            cstmt.execute();

            // Đọc kết quả message
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
    public void truncateAllNormalizedTables() throws SQLException {
        System.out.println("\nTruncating all normalized tables...");
        try (CallableStatement cstmt = connection.prepareCall("{CALL sp_truncate_normalized_tables()}")) {
            cstmt.execute();

            // Đọc kết quả message
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
    private int getOrInsertGenreId(String genreName) throws SQLException {
        try (CallableStatement cstmt = connection.prepareCall("{CALL sp_get_or_insert_genre(?, ?)}")) {
            cstmt.setString(1, genreName.trim());
            cstmt.registerOutParameter(2, Types.INTEGER);
            cstmt.execute();
            return cstmt.getInt(2);
        }
    }

    /**
     * Insert hoặc get ID của language bằng stored procedure
     */
    private int getOrInsertLanguageId(String languageName) throws SQLException {
        try (CallableStatement cstmt = connection.prepareCall("{CALL sp_get_or_insert_language(?, ?)}")) {
            cstmt.setString(1, languageName.trim());
            cstmt.registerOutParameter(2, Types.INTEGER);
            cstmt.execute();
            return cstmt.getInt(2);
        }
    }

    /**
     * Insert hoặc get ID của publisher bằng stored procedure
     */
    private Integer getOrInsertPublisherId(String publisherName) throws SQLException {
        if (publisherName == null || publisherName.trim().isEmpty()) {
            return null;
        }
        try (CallableStatement cstmt = connection.prepareCall("{CALL sp_get_or_insert_publisher(?, ?)}")) {
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
    private Integer getOrInsertDeveloperId(String developerName) throws SQLException {
        if (developerName == null || developerName.trim().isEmpty()) {
            return null;
        }
        try (CallableStatement cstmt = connection.prepareCall("{CALL sp_get_or_insert_developer(?, ?)}")) {
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
    private Integer getOrInsertSystemId(String systemName) throws SQLException {
        if (systemName == null || systemName.trim().isEmpty()) {
            return null;
        }
        try (CallableStatement cstmt = connection.prepareCall("{CALL sp_get_or_insert_system(?, ?)}")) {
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
    private long insertGame(String name, String priceUsd, String size, String support,
                            String ofPlayers, Integer systemId, Integer publisherId,
                            Integer developerId, String releaseDate, String source,
                             String fileName, int datesk) throws SQLException {
        try (CallableStatement cstmt = connection.prepareCall(
                "{CALL sp_insert_game(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?)}")) {

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

//            cstmt.setInt(12, datesk);
            cstmt.registerOutParameter(13, Types.BIGINT);

            cstmt.execute();

            return cstmt.getLong(13);


        }
    }

    /**
     * Insert game-genre relationship bằng stored procedure
     */
    private void insertGameGenre(long gameKey, int genreId) throws SQLException {
        try (CallableStatement cstmt = connection.prepareCall("{CALL sp_insert_game_genre(?, ?)}")) {
            cstmt.setLong(1, gameKey);
            cstmt.setInt(2, genreId);
            cstmt.execute();
        }
    }

    /**
     * Insert game-language relationship bằng stored procedure
     */
    private void insertGameLanguage(long gameKey, int languageId) throws SQLException {
        try (CallableStatement cstmt = connection.prepareCall("{CALL sp_insert_game_language(?, ?)}")) {
            cstmt.setLong(1, gameKey);
            cstmt.setInt(2, languageId);
            cstmt.execute();
        }
    }

    /**
     * Tách string thành list dựa trên delimiter
     */
    private List<String> splitAndTrim(String value, String delimiter) {
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
     * Normalize và load data từ nintendo_games table
     */
    public void normalizeAndLoad() throws SQLException {
        try {
            connection.setAutoCommit(false);

            // Tạo các normalized tables nếu chưa tồn tại
            createNormalizedTables();

            // XÓA TOÀN BỘ DỮ LIỆU CŨ
            truncateAllNormalizedTables();

            System.out.println("\nNormalizing data from nintendo_games...");

            // Đọc data từ source table
            String selectSql = "SELECT * FROM nintendo_games " +
                    "WHERE (is_deleted = 0 AND YEAR(load_expired) = 9999) " +
                    "ORDER BY id";

            int gameCount = 0;
            int genreCount = 0;
            int languageCount = 0;

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(selectSql)) {

                while (rs.next()) {
                    try {
                        // Get dimension IDs
                        Integer systemId = getOrInsertSystemId(rs.getString("system"));
                        Integer publisherId = getOrInsertPublisherId(rs.getString("publisher"));
                        Integer developerId = getOrInsertDeveloperId(rs.getString("developer"));

                        // Insert game và lấy game_key
                        long gameKey = insertGame(
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
                                rs.getInt("date_sk")
                        );
                        gameCount++;

                        // Xử lý genres
                        String genresStr = rs.getString("genre");
                        List<String> genres = splitAndTrim(genresStr, ",");
                        for (String genre : genres) {
                            int genreId = getOrInsertGenreId(genre);
                            insertGameGenre(gameKey, genreId);
                            genreCount++;
                        }

                        // Xử lý languages
                        String languagesStr = rs.getString("languages");
                        List<String> languages = splitAndTrim(languagesStr, ",");
                        for (String language : languages) {
                            int languageId = getOrInsertLanguageId(language);
                            insertGameLanguage(gameKey, languageId);
                            languageCount++;
                        }

                    } catch (Exception e) {
                        System.err.println("Error processing game: " + rs.getString("name"));
                        e.printStackTrace();
                    }
                }
            }

            connection.commit();
            System.out.println("\n" + "=".repeat(60));
            System.out.println("✓ Normalization Complete");
            System.out.println("=".repeat(60));
            System.out.println("Games loaded:                  " + gameCount);
            System.out.println("Game-Genre relationships:      " + genreCount);
            System.out.println("Game-Language relationships:   " + languageCount);
            System.out.println("=".repeat(60));

        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    public void loadCsvFile(String csvPath) throws SQLException, IOException {
        String tableName = "nintendo_games";
        String[] columns = {
                "id",
                "name",
                "price_usd",
                "size",
                "support",
                "of_players",
                "genre",
                "system",
                "publisher",
                "developer",
                "languages",
                "release_date",
                "release_year",
                "release_month",
                "source",
                "file_name",
                "load_ts",
                "load_expired",
                "load_staging",
                "is_deleted",
                "date_sk"
        };

        try(PreparedStatement pstmt = connection.prepareStatement("TRUNCATE TABLE " + tableName);) {
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        // Build SQL INSERT
        String colNames = Arrays.stream(columns)
                .map(col -> "`" + col + "`")
                .collect(Collectors.joining(","));
        String placeholders = String.join(",", Collections.nCopies(columns.length, "?"));
        String sql = "INSERT INTO " + tableName + " (" + colNames + ") VALUES (" + placeholders + ")";

        BufferedReader br = new BufferedReader(new FileReader(csvPath));
        PreparedStatement pstmt = connection.prepareStatement(sql);

        connection.setAutoCommit(false);

        // Bỏ qua dòng header
        br.readLine();

        String line;
        int count = 0;

        while ((line = br.readLine()) != null) {
            String[] data = line.split(";", -1);

            // price_usd - kiểm tra empty
            if (data[2] == null || data[2].trim().isEmpty()) {
                pstmt.setNull(3, Types.DECIMAL);
            } else {
                pstmt.setBigDecimal(3, new java.math.BigDecimal(data[2].trim()));
            }

            // Set giá trị cho từng cột
            pstmt.setLong(1, Long.parseLong(data[0])); // id
            pstmt.setString(2, data[1]); // name
            pstmt.setString(4, data[3]); // size
            pstmt.setString(5, data[4]); // support
            pstmt.setString(6, data[5]); // of_players
            pstmt.setString(7, data[6]); // genre
            pstmt.setString(8, data[7]); // system
            pstmt.setString(9, data[8]); // publisher
            pstmt.setString(10, data[9]); // developer
            pstmt.setString(11, data[10]); // languages

            // release_date
            if (data[11].isEmpty()) {
                pstmt.setNull(12, Types.DATE);
            } else {
                pstmt.setDate(12, Date.valueOf(data[11]));
            }

            // release_year
            if (data[12].isEmpty()) {
                pstmt.setNull(13, Types.INTEGER);
            } else {
                pstmt.setInt(13, Integer.parseInt(data[12]));
            }

            // release_month
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

        connection.commit();
        br.close();
        pstmt.close();

        System.out.println("Import thành công " + count + " bản ghi!");
    }

    /**
     * In thống kê dữ liệu đã normalize bằng stored procedure
     */
    public void printStatistics() throws SQLException {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("Database Statistics");
        System.out.println("=".repeat(60));

        try (CallableStatement cstmt = connection.prepareCall("{CALL sp_get_statistics()}")) {
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