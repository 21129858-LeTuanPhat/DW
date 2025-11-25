package datawarehouse.demo.service;

import datawarehouse.demo.api.GenreTotalDescSummary;
import datawarehouse.demo.entity.GenreMonthly;
import datawarehouse.demo.entity.GenreSummary;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.TypedQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class GenreSummaryServiceImpl implements GenreSummaryService {
    @PersistenceContext
    private EntityManager em;

    @Override
    public List<GenreTotalDescSummary> getListGameGenreTotalGameDesc() {
        String sql = "SELECT " +
                "genre_name, " +
                "total_games " +
                "FROM AGG_Genre_Summary " +
                "ORDER BY total_games DESC";

        // Thực thi native query
        List<Object[]> rows = em.createNativeQuery(sql).getResultList();

        // Map Object[] sang DTO
        return rows.stream().map(row -> new GenreTotalDescSummary(
                (String) row[0],             // genre_name
                ((Number) row[1]).longValue() // total_games
        )).toList();
    }
}
