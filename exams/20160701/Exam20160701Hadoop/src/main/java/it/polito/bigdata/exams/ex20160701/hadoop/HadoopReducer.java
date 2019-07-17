package it.polito.bigdata.exams.ex20160701.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        Double sum = 0.0;

        // Sum revenues
        for (DoubleWritable d : values) {
            sum += d.get();
        }

        // Emit if revenues are > 1M
        if (sum >= 1000000) context.write(key, NullWritable.get());
    }

}