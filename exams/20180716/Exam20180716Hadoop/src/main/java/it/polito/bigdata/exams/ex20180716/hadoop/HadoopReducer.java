package it.polito.bigdata.exams.ex20180716.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        Boolean ramFailure = false, hddFailure = false;

        // Loop through the values
        for (Text v : values) {
            if (v.toString().equals("RAM"))
                ramFailure = true;
            else if (v.toString().equals("Hard_disk"))
                hddFailure = true;
        }

        // Emit if both failures appeared
        if (ramFailure && hddFailure)
            context.write(key, NullWritable.get());
    }

}