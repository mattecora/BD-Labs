package it.polito.bigdata.lab08.ex01;

import java.io.Serializable;

public class Entry implements Serializable {
    private static final long serialVersionUID = -5417236762358053812L;
    
    private Integer station;
    private String dayOfTheWeek;
    private Integer hours, usedSlots, freeSlots, critical;

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

    public Integer getUsedSlots() {
        return usedSlots;
    }

    public void setUsedSlots(Integer usedSlots) {
        this.usedSlots = usedSlots;
    }

    public Integer getFreeSlots() {
        return freeSlots;
    }

    public void setFreeSlots(Integer freeSlots) {
        this.freeSlots = freeSlots;
    }

    public Integer getCritical() {
        return critical;
    }

    public void setCritical(Integer critical) {
        this.critical = critical;
    }
}