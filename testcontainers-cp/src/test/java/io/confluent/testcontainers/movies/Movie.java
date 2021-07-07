package io.confluent.testcontainers.movies;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class Movie {

    @JsonProperty("movie_id")
    private int movieId;

    @JsonProperty
    private String title;

    @JsonProperty("release_year")
    private int releaseYear;

    @JsonProperty("producer_id")
    private int producerId;

    public Movie() {
    }

    public Movie(int movieId, String title, int releaseYear, int producerId) {
        this.movieId = movieId;
        this.title = title;
        this.releaseYear = releaseYear;
        this.producerId = producerId;
    }

    public int getMovieId() {
        return movieId;
    }

    public String getTitle() {
        return title;
    }

    public int getReleaseYear() {
        return releaseYear;
    }

    public int getProducerId() {
        return producerId;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "movieId=" + movieId +
                ", title='" + title + '\'' +
                ", releaseYear=" + releaseYear +
                ", producerId=" + producerId +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Movie movie = (Movie) o;
        return movieId == movie.movieId && releaseYear == movie.releaseYear && producerId == movie.producerId && title.equals(movie.title);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movieId, title, releaseYear, producerId);
    }
}
