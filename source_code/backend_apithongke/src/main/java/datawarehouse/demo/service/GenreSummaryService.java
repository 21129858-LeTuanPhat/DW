package datawarehouse.demo.service;

import datawarehouse.demo.api.GenreTotalDescSummary;
import datawarehouse.demo.entity.GenreMonthly;
import datawarehouse.demo.entity.GenreSummary;

import java.util.List;

public interface GenreSummaryService {

    public List<GenreTotalDescSummary> getListGameGenreTotalGameDesc();

}
