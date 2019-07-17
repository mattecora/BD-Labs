package it.polito.bigdata.exams.ex20180626.spark;

import java.io.Serializable;

public class AboveAndUnder implements Serializable {
    private static final long serialVersionUID = -4968363975259120271L;
    private Integer above;
    private Integer under;

    public AboveAndUnder(Integer above, Integer under) {
        this.above = above;
        this.under = under;
    }

    public Integer getAbove() {
        return above;
    }

    public void setAbove(Integer above) {
        this.above = above;
    }

    public Integer getUnder() {
        return under;
    }

    public void setUnder(Integer under) {
        this.under = under;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((above == null) ? 0 : above.hashCode());
        result = prime * result + ((under == null) ? 0 : under.hashCode());
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
        AboveAndUnder other = (AboveAndUnder) obj;
        if (above == null) {
            if (other.above != null)
                return false;
        } else if (!above.equals(other.above))
            return false;
        if (under == null) {
            if (other.under != null)
                return false;
        } else if (!under.equals(other.under))
            return false;
        return true;
    }
}