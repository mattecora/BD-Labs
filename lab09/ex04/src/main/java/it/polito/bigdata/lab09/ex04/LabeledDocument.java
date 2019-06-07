package it.polito.bigdata.lab09.ex04;

import java.io.Serializable;

public class LabeledDocument implements Serializable {
    private static final long serialVersionUID = -5272435571194825646L;
    
    private Double label;
    private String text;

    public LabeledDocument(Double label, String text) {
        this.label = label;
        this.text = text;
    }

    public Double getLabel() {
        return label;
    }

    public void setLabel(Double label) {
        this.label = label;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}