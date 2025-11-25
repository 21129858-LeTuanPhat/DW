package datawarehouse.demo.api;

public class GenreTotalDescSummary {
    private String genreName;
    private long totalGames;

    public GenreTotalDescSummary(String genreName, long totalGames) {
        this.genreName = genreName;
        this.totalGames = totalGames;
    }

    public String getGenreName() { return genreName; }
    public long getTotalGames() { return totalGames; }


}
