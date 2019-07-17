package it.polito.bigdata.exams.ex20170714.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    private final static int HIGH = 1;
    private final static int LOW = 2;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        Boolean high = false, low = false;

        // Iterate over values to update flags
        for (IntWritable i : values) {
            if (i.get() == HIGH) high = true;
            else if (i.get() == LOW) low = true;
        }

        // Emit only if both are positive
        if (high && low) context.write(key, NullWritable.get());
    }

}