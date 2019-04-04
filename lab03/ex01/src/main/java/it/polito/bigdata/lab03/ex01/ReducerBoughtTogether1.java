package it.polito.bigdata.lab03.ex01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerBoughtTogether1 extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int occurrences = 0;

        // Count occurrences
        for (IntWritable i : values) {
            occurrences = occurrences + i.get();
        }

        // Emit (pair, count)
        context.write(key, new IntWritable(occurrences));
    }

}
