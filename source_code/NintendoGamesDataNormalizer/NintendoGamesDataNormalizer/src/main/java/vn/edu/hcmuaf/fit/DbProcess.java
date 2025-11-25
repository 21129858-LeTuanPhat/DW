package vn.edu.hcmuaf.fit;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.mail.MessagingException;
import vn.edu.hcmuaf.fit.config.DbConfig;
import vn.edu.hcmuaf.fit.config.JsonConfig;

import java.io.File;
import java.sql.*;
import java.time.LocalDate;

public class DbProcess {
    private static DbConfig m_DbConfig = null;

    private DbProcess() {
    }

    public static void setConfig(DbConfig dbConfig) {
        m_DbConfig = dbConfig;
    }

    public static void setConfig(JsonConfig jsonConfig) {
        m_DbConfig = new DbConfig();
        m_DbConfig.setServerIp(jsonConfig.getIp());
        m_DbConfig.setPort(jsonConfig.getPort());
        m_DbConfig.setName(jsonConfig.getDatabase());
        m_DbConfig.setUser(jsonConfig.getUser());
        m_DbConfig.setPassword(jsonConfig.getPassword());
    }

    private static Connection getConnection() throws SQLException {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s", m_DbConfig.getServerIp(), m_DbConfig.getPort(), m_DbConfig.getName());
        return DriverManager.getConnection(jdbcUrl, m_DbConfig.getUser(), m_DbConfig.getPassword());
    }


    public static long insertLog(String name, String status, String description, int jobId) throws SQLException {
        String Sql = """
                    INSERT INTO process_log (name, description, status, created_at, updated_at, job_id)
                    VALUES (?, ?, ?, NOW(), NOW(), ?)
                """;
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(Sql, Statement.RETURN_GENERATED_KEYS);) {
            pstmt.setString(1, name);
            pstmt.setString(2, description);
            pstmt.setString(3, status);
            pstmt.setInt(4, jobId);

            int affected = pstmt.executeUpdate();
            if (affected == 0) {
                throw new SQLException("Insert failed, no rows affected.");
            }

            try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                } else {
                    throw new SQLException("Insert succeeded but no ID obtained.");
                }
            }
        }
    }

    public static void updateLog(long processId, String status) {
        String Sql = """
                UPDATE process_log
                SET status = ?,
                    updated_at = NOW()
                WHERE id = ?
                """;
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(Sql);) {
            pstmt.setLong(2, processId);
            pstmt.setString(1, status);
            int affected = pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean checkHasProcessAndNotError() {
        LocalDate today = LocalDate.now();
        String todayStr = today.toString();

        String query = """
                            SELECT
                            COUNT(*) as total_processes,
                            SUM(CASE WHEN status NOT IN ('PROCESSING', 'ERROR') THEN 1 ELSE 0 END) as valid_processes
                            FROM process_log
                            WHERE DATE(created_at) = ?
                        """;

        try (PreparedStatement pstmt = getConnection().prepareStatement(query)) {
            pstmt.setString(1, todayStr);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    int totalProcesses = rs.getInt("total_processes");
                    int validProcesses = rs.getInt("valid_processes");

                    if (totalProcesses == 0) {
                        return false;
                    }

                    return validProcesses != totalProcesses;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return false;
    }

    public static JsonConfig getConfigJson(String filePath) throws MessagingException {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonConfig config = mapper.readValue(new File(filePath), JsonConfig.class);
            System.out.println(config);
            System.out.println("IP: " + config.getIp());
            System.out.println("Port: " + config.getPort());
            System.out.println("Database: " + config.getDatabase());
            return config;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
