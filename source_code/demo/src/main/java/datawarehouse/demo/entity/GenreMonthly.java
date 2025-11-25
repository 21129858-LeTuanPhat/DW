package datawarehouse.demo.entity;

import jakarta.persistence.*;

import java.sql.Date;

@Entity
@Table(name="agg_genre_monthly")
public class GenreMonthly {

    @Id
    @Column(name = "agg_id")
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private int id;

    @Column(name = "genre_id")
    private int genre_id;

    @Column(name = "genre_name")
    private String genre_name;

    @Column(name = "period_date")
    private Date period_date;

    @Column(name = "games_released")
    private int games_released;

    @Column(name = "avg_price")
    private double avg_price;

    @Column(name = "cumulative_games_ytd")
    private int cumulative_games_ytd;

    @Column(name = "last_updated")
    private Date last_updated;

    public GenreMonthly() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getGenre_id() {
        return genre_id;
    }

    public void setGenre_id(int genre_id) {
        this.genre_id = genre_id;
    }

    public String getGenre_name() {
        return genre_name;
    }

    public void setGenre_name(String genre_name) {
        this.genre_name = genre_name;
    }

    public Date getPeriod_date() {
        return period_date;
    }

    public void setPeriod_date(Date period_date) {
        this.period_date = period_date;
    }

    public int getGames_released() {
        return games_released;
    }

    public void setGames_released(int games_released) {
        this.games_released = games_released;
    }

    public double getAvg_price() {
        return avg_price;
    }

    public void setAvg_price(double avg_price) {
        this.avg_price = avg_price;
    }

    public int getCumulative_games_ytd() {
        return cumulative_games_ytd;
    }

    public void setCumulative_games_ytd(int cumulative_games_ytd) {
        this.cumulative_games_ytd = cumulative_games_ytd;
    }

    public Date getLast_updated() {
        return last_updated;
    }

    public void setLast_updated(Date last_updated) {
        this.last_updated = last_updated;
    }

    @Override
    public String toString() {
        return "GenreMonthly{" +
                "id=" + id +
                ", genre_id=" + genre_id +
                ", genre_name='" + genre_name + '\'' +
                ", period_date=" + period_date +
                ", games_released=" + games_released +
                ", avg_price=" + avg_price +
                ", cumulative_games_ytd=" + cumulative_games_ytd +
                ", last_updated=" + last_updated +
                '}';
    }
}
