package it.polito.bigdata.exams.ex20190702.spark;

import java.io.Serializable;

public class BikeFailureMonth implements Serializable {
    private static final long serialVersionUID = -1556599430882593347L;
    
    private String bike;
    private String month;

    public BikeFailureMonth(String bike, String month) {
        this.bike = bike;
        this.month = month;
    }

    public String getBike() {
        return bike;
    }

    public void setBike(String bike) {
        this.bike = bike;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bike == null) ? 0 : bike.hashCode());
        result = prime * result + ((month == null) ? 0 : month.hashCode());
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
        BikeFailureMonth other = (BikeFailureMonth) obj;
        if (bike == null) {
            if (other.bike != null)
                return false;
        } else if (!bike.equals(other.bike))
            return false;
        if (month == null) {
            if (other.month != null)
                return false;
        } else if (!month.equals(other.month))
            return false;
        return true;
    }
}