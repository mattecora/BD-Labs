package it.polito.bigdata.lab04.ex01;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerItemsRating2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        int n = 0;
        double avg = 0;

        // Compute the average rating
        for (DoubleWritable v : values) {
            n++;
            avg = avg + v.get();
        }
        avg = avg / n;

        // Emit a pair (item, average rating)
        context.write(key, new DoubleWritable(avg));
    }
}
