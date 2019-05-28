package it.polito.bigdata.lab08.ex01;

import java.io.Serializable;

public class CriticalityEntry implements Serializable {
    private static final long serialVersionUID = -3659337705064072123L;

    private Integer station;
    private String dayOfTheWeek;
    private Integer hours;
    private Double criticality;

    public Integer getStation() {
        return station;
    }

    public void setStation(Integer station) {
        this.station = station;
    }

    public String getDayOfTheWeek() {
        return dayOfTheWeek;
    }

    public void setDayOfTheWeek(String dayOfTheWeek) {
        this.dayOfTheWeek = dayOfTheWeek;
    }

    public Integer getHours() {
        return hours;
    }

    public void setHours(Integer hours) {
        this.hours = hours;
    }

    public Double getCriticality() {
        return criticality;
    }

    public void setCriticality(Double criticality) {
        this.criticality = criticality;
    }
}