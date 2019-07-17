package it.polito.bigdata.exams.ex20160919.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        Integer sum = 0;

        // Sum number of readings
        for (IntWritable i : values) {
            sum = sum + i.get();
        }

        // Check if greater than 30
        if (sum >= 30) {
            // Emit a pair (requested format, null)
            context.write(new Text(key.toString() + "," + sum), NullWritable.get());
        }
    }

}