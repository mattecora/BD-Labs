package it.polito.bigdata.lab07.ex01;

import java.io.Serializable;

public class Average implements Serializable {
    private static final long serialVersionUID = -897887255984046928L;
    private Integer sum;
    private Integer total;

    public Average() {
        sum = 0;
        total = 0;
    }

    public Average(Average other) {
        sum = other.sum;
        total = other.total;
    }

    public Integer getSum() {
        return sum;
    }

    public Integer getTotal() {
        return total;
    }

    public Double getAverage() {
        return (double) sum / total;
    }

    public void addSum(Integer qty) {
        sum += qty;
    }

    public void addTotal(Integer qty) {
        total += qty;
    }
}