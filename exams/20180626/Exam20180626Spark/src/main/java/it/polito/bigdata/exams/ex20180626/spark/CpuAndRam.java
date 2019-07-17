package it.polito.bigdata.exams.ex20180626.spark;

import java.io.Serializable;

public class CpuAndRam implements Serializable {
    private static final long serialVersionUID = -5010177847704446676L;
    private Integer count;
    private Double cpu;
    private Double ram;

    public CpuAndRam(Integer count, Double cpu, Double ram) {
        this.count = count;
        this.cpu = cpu;
        this.ram = ram;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Double getCpu() {
        return cpu;
    }

    public void setCpu(Double cpu) {
        this.cpu = cpu;
    }

    public Double getRam() {
        return ram;
    }

    public void setRam(Double ram) {
        this.ram = ram;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cpu == null) ? 0 : cpu.hashCode());
        result = prime * result + ((ram == null) ? 0 : ram.hashCode());
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
        CpuAndRam other = (CpuAndRam) obj;
        if (cpu == null) {
            if (other.cpu != null)
                return false;
        } else if (!cpu.equals(other.cpu))
            return false;
        if (ram == null) {
            if (other.ram != null)
                return false;
        } else if (!ram.equals(other.ram))
            return false;
        return true;
    }
}