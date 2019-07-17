package it.polito.bigdata.exams.ex20170630.spark;

import java.io.Serializable;

public class WeekMaxAndMin implements Serializable {
    private static final long serialVersionUID = -3375962591334206892L;
    private Double maxFirst, maxLast;
    private String dayFirst, dayLast;

    public WeekMaxAndMin(Double maxFirst, Double maxLast, String dayFirst, String dayLast) {
        this.maxFirst = maxFirst;
        this.maxLast = maxLast;
        this.dayFirst = dayFirst;
        this.dayLast = dayLast;
    }

    public Double getMaxFirst() {
        return maxFirst;
    }

    public void setMaxFirst(Double maxFirst) {
        this.maxFirst = maxFirst;
    }

    public Double getMaxLast() {
        return maxLast;
    }

    public void setMaxLast(Double maxLast) {
        this.maxLast = maxLast;
    }

    public String getDayFirst() {
        return dayFirst;
    }

    public void setDayFirst(String dayFirst) {
        this.dayFirst = dayFirst;
    }

    public String getDayLast() {
        return dayLast;
    }

    public void setDayLast(String dayLast) {
        this.dayLast = dayLast;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dayFirst == null) ? 0 : dayFirst.hashCode());
        result = prime * result + ((dayLast == null) ? 0 : dayLast.hashCode());
        result = prime * result + ((maxFirst == null) ? 0 : maxFirst.hashCode());
        result = prime * result + ((maxLast == null) ? 0 : maxLast.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        WeekMaxAndMin other = (WeekMaxAndMin) obj;
        if (dayFirst == null) {
            if (other.dayFirst != null)
                return false;
        } else if (!dayFirst.equals(other.dayFirst))
            return false;
        if (dayLast == null) {
            if (other.dayLast != null)
                return false;
        } else if (!dayLast.equals(other.dayLast))
            return false;
        if (maxFirst == null) {
            if (other.maxFirst != null)
                return false;
        } else if (!maxFirst.equals(other.maxFirst))
            return false;
        if (maxLast == null) {
            if (other.maxLast != null)
                return false;
        } else if (!maxLast.equals(other.maxLast))
            return false;
        return true;
    }
}