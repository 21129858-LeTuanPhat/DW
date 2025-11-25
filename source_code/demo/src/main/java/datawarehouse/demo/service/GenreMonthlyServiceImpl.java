package datawarehouse.demo.service;

import datawarehouse.demo.api.GenreBestWorstMonth;
import datawarehouse.demo.api.GenreSeasonSummary;
import datawarehouse.demo.api.GenreTop10Release;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class GenreMonthlyServiceImpl implements GenreMonthlyService {
    @PersistenceContext
    private EntityManager em;



    @Override
    public List<GenreSeasonSummary> getGameFollowSeason() {
        String sql = "SELECT " +
                "genre_name, " +
                "CASE " +
                "   WHEN MONTH(period_date) IN (12,1,2) THEN 'Q4-Q1 (Holiday)' " +
                "   WHEN MONTH(period_date) IN (3,4,5) THEN 'Q1-Q2 (Spring)' " +
                "   WHEN MONTH(period_date) IN (6,7,8) THEN 'Q2-Q3 (Summer)' " +
                "   ELSE 'Q3-Q4 (Fall)' " +
                "END AS season, " +
                "SUM(games_released) AS total_games, " +
                "ROUND(AVG(avg_price),2) AS avg_price " +
                "FROM AGG_Genre_Monthly " +
                "WHERE YEAR(period_date) >= YEAR(CURDATE()) - 2 " +
                "GROUP BY genre_name, season " +
                "ORDER BY genre_name, total_games DESC";
        List<Object[]> rows = em.createNativeQuery(sql).getResultList();
        return rows.stream().map(row -> new GenreSeasonSummary(
                (String) row[0],
                (String) row[1],
                ((Number) row[2]).longValue(),
                ((Number) row[3]).doubleValue()
        )).toList();
    }

    @Override
    public List<GenreTop10Release> getTop10Games() {
        String sql = "SELECT " +
                "DATE_FORMAT(period_date, '%Y-%m') AS month, " +
                "SUM(games_released) AS total_games, " +
                "COUNT(DISTINCT genre_id) AS active_genres, " +
                "ROUND(AVG(avg_price), 2) AS avg_price, " +
                "DENSE_RANK() OVER (ORDER BY SUM(games_released) DESC) AS rank_by_volume " +
                "FROM AGG_Genre_Monthly " +
                "GROUP BY period_date " +
                "ORDER BY total_games DESC " +
                "LIMIT 10";

        List<Object[]> rows = em.createNativeQuery(sql).getResultList();
        return rows.stream().map(row -> new GenreTop10Release(
                (String) row[0],
                ((Number) row[1]).longValue(),
                ((Number) row[2]).longValue(),
                ((Number) row[3]).doubleValue(),
                ((Number) row[4]).intValue()
        )).toList();
    }

    @Override
    public List<GenreBestWorstMonth> getBestAndWorstMonth() {
        String sql = "WITH ranked_months AS (" +
                "    SELECT " +
                "        genre_name, " +
                "        DATE_FORMAT(period_date, '%Y-%m') AS month, " +
                "        games_released, " +
                "        ROW_NUMBER() OVER (PARTITION BY genre_id ORDER BY games_released DESC) AS rank_high, " +
                "        ROW_NUMBER() OVER (PARTITION BY genre_id ORDER BY games_released ASC) AS rank_low " +
                "    FROM AGG_Genre_Monthly " +
                "    WHERE period_date >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH) " +
                ") " +
                "SELECT " +
                "    genre_name, " +
                "    MAX(CASE WHEN rank_high = 1 THEN CONCAT(month, ' (', games_released, ' games)') END) AS best_month, " +
                "    MAX(CASE WHEN rank_low = 1 THEN CONCAT(month, ' (', games_released, ' games)') END) AS worst_month " +
                "FROM ranked_months " +
                "WHERE rank_high = 1 OR rank_low = 1 " +
                "GROUP BY genre_name " +
                "ORDER BY genre_name";

        Query query = em.createNativeQuery(sql);

        List<Object[]> rows = query.getResultList();

        // Map Object[] → DTO
        return rows.stream().map(row -> new GenreBestWorstMonth(
                (String) row[0], // genre_name
                (String) row[1], // best_month
                (String) row[2]  // worst_month
        )).toList();
    }
}
