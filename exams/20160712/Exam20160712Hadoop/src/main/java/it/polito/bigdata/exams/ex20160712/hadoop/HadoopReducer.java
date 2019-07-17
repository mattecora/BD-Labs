package it.polito.bigdata.exams.ex20160712.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        Integer sum = 0;

        // Sum the number of large stations
        for (IntWritable i : values)
            sum = sum + i.get();
        
        // Emit a pair (zone, large stations)
        context.write(key, new IntWritable(sum));
    }

}