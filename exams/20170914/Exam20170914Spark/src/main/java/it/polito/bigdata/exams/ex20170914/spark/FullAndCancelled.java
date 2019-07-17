package it.polito.bigdata.exams.ex20170914.spark;

import java.io.Serializable;

public class FullAndCancelled implements Serializable {
    private static final long serialVersionUID = 4299693236330563824L;
    private Integer full;
    private Integer cancelled;
    private Integer total;

    public FullAndCancelled(Integer full, Integer cancelled, Integer total) {
        this.full = full;
        this.cancelled = cancelled;
        this.total = total;
    }

    public Integer getFull() {
        return full;
    }

    public void setFull(Integer full) {
        this.full = full;
    }

    public Integer getCancelled() {
        return cancelled;
    }

    public void setCancelled(Integer cancelled) {
        this.cancelled = cancelled;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cancelled == null) ? 0 : cancelled.hashCode());
        result = prime * result + ((full == null) ? 0 : full.hashCode());
        result = prime * result + ((total == null) ? 0 : total.hashCode());
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
        FullAndCancelled other = (FullAndCancelled) obj;
        if (cancelled == null) {
            if (other.cancelled != null)
                return false;
        } else if (!cancelled.equals(other.cancelled))
            return false;
        if (full == null) {
            if (other.full != null)
                return false;
        } else if (!full.equals(other.full))
            return false;
        if (total == null) {
            if (other.total != null)
                return false;
        } else if (!total.equals(other.total))
            return false;
        return true;
    }
}