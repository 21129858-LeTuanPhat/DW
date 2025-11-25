package datawarehouse.demo.api;


public class GenreSeasonSummary {
    private String genreName;
    private String season;
    private long totalGames;
    private double avgPrice;

    public GenreSeasonSummary(String genreName, String season, long totalGames, double avgPrice) {
        this.genreName = genreName;
        this.season = season;
        this.totalGames = totalGames;
        this.avgPrice = avgPrice;
    }

    public String getGenreName() { return genreName; }
    public String getSeason() { return season; }
    public long getTotalGames() { return totalGames; }
    public double getAvgPrice() { return avgPrice; }
}
