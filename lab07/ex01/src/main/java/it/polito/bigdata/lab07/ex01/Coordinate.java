package it.polito.bigdata.lab07.ex01;

import java.io.Serializable;

public class Coordinate implements Serializable {
    private static final long serialVersionUID = 4478696234634593409L;
    private Double latitude;
    private Double longitude;

    public Coordinate(Double latitude, Double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getLongitude() {
        return longitude;
    }
}