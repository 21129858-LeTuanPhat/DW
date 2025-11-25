package datawarehouse.demo.api;

public class GenreBestWorstMonth {
    private String genreName;
    private String bestMonth;
    private String worstMonth;

    public GenreBestWorstMonth(String genreName, String bestMonth, String worstMonth) {
        this.genreName = genreName;
        this.bestMonth = bestMonth;
        this.worstMonth = worstMonth;
    }

    public String getGenreName() { return genreName; }
    public String getBestMonth() { return bestMonth; }
    public String getWorstMonth() { return worstMonth; }
}
