package it.polito.bigdata.exams.ex20170630.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, Text>.Context context)
            throws IOException, InterruptedException {
        Double max = Double.MIN_VALUE, min = Double.MAX_VALUE;

        // Loop through the values
        for (DoubleWritable v : values) {
            // Update max and min
            if (v.get() > max) max = v.get();
            if (v.get() < min) min = v.get();
        }

        // Compute absolute and relative difference
        Double absDiff = max - min;
        Double relDiff = absDiff / min;

        // Emit a pair if relative difference > 5%
        if (relDiff > 0.05) context.write(key, new Text(absDiff + "," + relDiff));
    }

}