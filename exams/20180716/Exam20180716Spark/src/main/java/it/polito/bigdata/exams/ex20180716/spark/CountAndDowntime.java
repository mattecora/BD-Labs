package it.polito.bigdata.exams.ex20180716.spark;

import java.io.Serializable;

public class CountAndDowntime implements Serializable {
    private static final long serialVersionUID = -4899350919794448596L;
    private Integer count;
    private Integer downtime;

    public CountAndDowntime(Integer count, Integer downtime) {
        this.count = count;
        this.downtime = downtime;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Integer getDowntime() {
        return downtime;
    }

    public void setDowntime(Integer downtime) {
        this.downtime = downtime;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((count == null) ? 0 : count.hashCode());
        result = prime * result + ((downtime == null) ? 0 : downtime.hashCode());
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
        CountAndDowntime other = (CountAndDowntime) obj;
        if (count == null) {
            if (other.count != null)
                return false;
        } else if (!count.equals(other.count))
            return false;
        if (downtime == null) {
            if (other.downtime != null)
                return false;
        } else if (!downtime.equals(other.downtime))
            return false;
        return true;
    }
}