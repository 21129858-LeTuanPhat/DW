package datawarehouse.demo.entity;

import jakarta.persistence.*;

import javax.naming.Name;
import java.sql.Date;

@Entity
@Table(name = "agg_genre_summary")
public class GenreSummary {

    @Id
    @Column(name = "genre_id")
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private int id;

    @Column(name = "genre_name")
    private String genre_name;

    @Column(name = "total_games")
    private int total_games;

    @Column(name = "total_games_paid")
    private int total_games_paid;

    @Column(name = "avg_price")
    private double avg_price;

    @Column(name = "min_price")
    private double min_price;

    @Column(name = "max_price")
    private double max_price;

    @Column(name = "median_price")
    private double median_price;

    @Column(name = "avg_languages")
    private int avg_languages;

    @Column(name = "pct_multi_genre")
    private double pct_multi_genre;

    @Column(name = "latest_release_year")
    private int latest_release_year;

    @Column(name = "total_publishers")
    private int total_publishers;

    @Column(name = "last_updated")
    private Date last_updated;

    @Column(name = "record_count")
    private int record_count;

    public GenreSummary() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getGenre_name() {
        return genre_name;
    }

    public void setGenre_name(String genre_name) {
        this.genre_name = genre_name;
    }

    public int getTotal_games() {
        return total_games;
    }

    public void setTotal_games(int total_games) {
        this.total_games = total_games;
    }

    public int getTotal_games_paid() {
        return total_games_paid;
    }

    public void setTotal_games_paid(int total_games_paid) {
        this.total_games_paid = total_games_paid;
    }

    public double getAvg_price() {
        return avg_price;
    }

    public void setAvg_price(double avg_price) {
        this.avg_price = avg_price;
    }

    public double getMin_price() {
        return min_price;
    }

    public void setMin_price(double min_price) {
        this.min_price = min_price;
    }

    public double getMax_price() {
        return max_price;
    }

    public void setMax_price(double max_price) {
        this.max_price = max_price;
    }

    public double getMedian_price() {
        return median_price;
    }

    public void setMedian_price(double median_price) {
        this.median_price = median_price;
    }

    public int getAvg_languages() {
        return avg_languages;
    }

    public void setAvg_languages(int avg_languages) {
        this.avg_languages = avg_languages;
    }

    public double getPct_multi_genre() {
        return pct_multi_genre;
    }

    public void setPct_multi_genre(double pct_multi_genre) {
        this.pct_multi_genre = pct_multi_genre;
    }

    public int getLatest_release_year() {
        return latest_release_year;
    }

    public void setLatest_release_year(int latest_release_year) {
        this.latest_release_year = latest_release_year;
    }

    public int getTotal_publishers() {
        return total_publishers;
    }

    public void setTotal_publishers(int total_publishers) {
        this.total_publishers = total_publishers;
    }

    public Date getLast_updated() {
        return last_updated;
    }

    public void setLast_updated(Date last_updated) {
        this.last_updated = last_updated;
    }

    public int getRecord_count() {
        return record_count;
    }

    public void setRecord_count(int record_count) {
        this.record_count = record_count;
    }

    @Override
    public String toString() {
        return "GenreSummary{" +
                "id=" + id +
                ", genre_name='" + genre_name + '\'' +
                ", total_games=" + total_games +
                ", total_games_paid=" + total_games_paid +
                ", avg_price=" + avg_price +
                ", min_price=" + min_price +
                ", max_price=" + max_price +
                ", median_price=" + median_price +
                ", avg_languages=" + avg_languages +
                ", pct_multi_genre=" + pct_multi_genre +
                ", latest_release_year=" + latest_release_year +
                ", total_publishers=" + total_publishers +
                ", last_updated=" + last_updated +
                ", record_count=" + record_count +
                '}';
    }
}
