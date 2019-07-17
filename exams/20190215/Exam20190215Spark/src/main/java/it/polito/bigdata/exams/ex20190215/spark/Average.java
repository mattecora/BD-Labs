package it.polito.bigdata.exams.ex20190215.spark;

public class Average {
    private Integer sum;
    private Integer count;

    public Average(Integer sum, Integer count) {
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

    public Double getAverage() {
        return (double) sum / count;
    }
}