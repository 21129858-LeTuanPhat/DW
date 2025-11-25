package datawarehouse.demo.rest;


import datawarehouse.demo.api.GenreTotalDescSummary;
import datawarehouse.demo.service.GenreSummaryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/genresummary")
@CrossOrigin(origins = "*")
public class GenreSummaryController {


    private  GenreSummaryService genreService;

    @Autowired
    public GenreSummaryController(GenreSummaryService genreService) {
        this.genreService = genreService;
    }

    @GetMapping("/getGenresByTotalGamesDesc")
    public List<GenreTotalDescSummary> getGenresByTotalGamesDesc() {
        return genreService.getListGameGenreTotalGameDesc();
    }
}
