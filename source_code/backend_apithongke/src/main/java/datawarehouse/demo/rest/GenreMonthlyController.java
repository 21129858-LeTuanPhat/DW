package datawarehouse.demo.rest;

import datawarehouse.demo.api.GenreBestWorstMonth;
import datawarehouse.demo.api.GenreSeasonSummary;
import datawarehouse.demo.api.GenreTop10Release;
import datawarehouse.demo.service.GenreMonthlyService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/genremonthly")
@CrossOrigin(origins = "*")
public class GenreMonthlyController {

    private final GenreMonthlyService service;

    public GenreMonthlyController(GenreMonthlyService service) {
        this.service = service;
    }
    @GetMapping("/season-summary")
    public List<GenreSeasonSummary> getGameFollowSeason() {
        return service.getGameFollowSeason();
    }
    @GetMapping("/top10release")
    public List<GenreTop10Release> getTop10Games() {
        return service.getTop10Games();
    }
    @GetMapping("/bestandworst-month")
    public List<GenreBestWorstMonth> getBestAndWorstMonth() {
        return service.getBestAndWorstMonth();
    }

}
