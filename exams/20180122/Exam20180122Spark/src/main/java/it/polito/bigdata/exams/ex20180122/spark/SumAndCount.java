package it.polito.bigdata.exams.ex20180122.spark;

import java.io.Serializable;

public class SumAndCount implements Serializable {
    private static final long serialVersionUID = -5803969309103408766L;
    private Integer sum;
    private Integer count;

    public SumAndCount(Integer sum, Integer count) {
        this.sum = sum;
        this.count = count;
    }

    public Integer getSum() {
        return sum;
    }

    public void setSum(Integer sum) {
        this.sum = sum;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((count == null) ? 0 : count.hashCode());
        result = prime * result + ((sum == null) ? 0 : sum.hashCode());
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
        SumAndCount other = (SumAndCount) obj;
        if (count == null) {
            if (other.count != null)
                return false;
        } else if (!count.equals(other.count))
            return false;
        if (sum == null) {
            if (other.sum != null)
                return false;
        } else if (!sum.equals(other.sum))
            return false;
        return true;
    }
}