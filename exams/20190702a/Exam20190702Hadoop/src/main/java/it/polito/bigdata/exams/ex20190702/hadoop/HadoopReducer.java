package it.polito.bigdata.exams.ex20190702.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        Text lastValue = null;
        boolean valid = true;

        // Loop through the value to check if two are different
        for (Text value : values) {
            if (lastValue != null && !value.equals(lastValue)) {
                valid = false;
                break;
            }

            lastValue = value;
        }

        // If valid, emit the input pair
        if (valid)
            context.write(key, lastValue);
    }

}