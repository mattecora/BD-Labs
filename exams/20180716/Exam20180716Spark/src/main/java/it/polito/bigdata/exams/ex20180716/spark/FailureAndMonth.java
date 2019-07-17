package it.polito.bigdata.exams.ex20180716.spark;

import java.io.Serializable;

public class FailureAndMonth implements Serializable {
    private static final long serialVersionUID = -601474035239124846L;
    private String server;
    private Integer month;

    public FailureAndMonth(String server, Integer month) {
        this.server = server;
        this.month = month;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((month == null) ? 0 : month.hashCode());
        result = prime * result + ((server == null) ? 0 : server.hashCode());
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
        FailureAndMonth other = (FailureAndMonth) obj;
        if (month == null) {
            if (other.month != null)
                return false;
        } else if (!month.equals(other.month))
            return false;
        if (server == null) {
            if (other.server != null)
                return false;
        } else if (!server.equals(other.server))
            return false;
        return true;
    }
}