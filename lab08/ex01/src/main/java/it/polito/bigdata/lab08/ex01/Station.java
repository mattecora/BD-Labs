package it.polito.bigdata.lab08.ex01;

import java.io.Serializable;

public class Station implements Serializable {
    private static final long serialVersionUID = 6723209882078800900L;

    private Integer id;
    private Double longitude, latitude;
    private String name;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}