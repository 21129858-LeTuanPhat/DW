package datawarehouse.demo.api;

public class GenreTop10Release {
    private String month;
    private long totalGames;
    private long activeGenres;
    private double avgPrice;
    private int rankByVolume;

    public GenreTop10Release(String month, long totalGames, long activeGenres, double avgPrice, int rankByVolume) {
        this.month = month;
        this.totalGames = totalGames;
        this.activeGenres = activeGenres;
        this.avgPrice = avgPrice;
        this.rankByVolume = rankByVolume;
    }

    public String getMonth() { return month; }
    public long getTotalGames() { return totalGames; }
    public long getActiveGenres() { return activeGenres; }
    public double getAvgPrice() { return avgPrice; }
    public int getRankByVolume() { return rankByVolume; }
}

