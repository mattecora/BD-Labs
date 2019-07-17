package it.polito.bigdata.exams.ex20180626.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        Integer count = 0;

        // Count values
        for (NullWritable n : values)
            count++;

        // Emit if count is at least 10000
        if (count >= 10000)
            context.write(key, NullWritable.get());
    }

}