package datawarehouse.demo.service;

import datawarehouse.demo.api.GenreBestWorstMonth;
import datawarehouse.demo.api.GenreSeasonSummary;
import datawarehouse.demo.api.GenreTop10Release;

import java.util.List;

public interface GenreMonthlyService {

    public List<GenreSeasonSummary> getGameFollowSeason();

    public List<GenreTop10Release> getTop10Games();

    public List<GenreBestWorstMonth> getBestAndWorstMonth();
}
