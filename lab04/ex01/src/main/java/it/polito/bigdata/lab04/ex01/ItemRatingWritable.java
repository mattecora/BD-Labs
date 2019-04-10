package it.polito.bigdata.lab04.ex01;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ItemRatingWritable implements Writable {
    private String item;
    private int rating;

    public ItemRatingWritable() {

    }

    public ItemRatingWritable(String item, int rating) {
        this.item = item;
        this.rating = rating;
    }

    public String getItem() {
        return item;
    }

    public int getRating() {
        return rating;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.item);
        out.writeInt(rating);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.item = in.readUTF();
        this.rating = in.readInt();
    }

    @Override
    public String toString() {
        return this.item + " " + this.rating;
    }
}
