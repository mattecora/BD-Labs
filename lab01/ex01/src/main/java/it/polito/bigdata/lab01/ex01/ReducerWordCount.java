package it.polito.bigdata.lab01.ex01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerWordCount extends Reducer<
        Text,           // Input key type
        IntWritable,    // Input value type
        Text,           // Output key type
        IntWritable> {  // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<IntWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        int occurrences = 0;

        // Iterate over the set of values and sum them
        for (IntWritable value : values) {
            occurrences = occurrences + value.get();
        }

        context.write(key, new IntWritable(occurrences));
    }
}