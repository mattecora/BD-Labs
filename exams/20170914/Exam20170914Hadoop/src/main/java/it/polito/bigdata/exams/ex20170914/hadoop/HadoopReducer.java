package it.polito.bigdata.exams.ex20170914.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, BooleanWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<BooleanWritable> values, Reducer<Text, BooleanWritable, Text, DoubleWritable>.Context context)
            throws IOException, InterruptedException {
        Long sumCancelled = 0L, sumTotal = 0L;
        Double propCancelled;

        // Loop through the values
        for (BooleanWritable v : values) {
            // Update sum of cancelled
            if (v.get()) sumCancelled++;

            // Update sum
            sumTotal++;
        }

        // Compute percentage of cancelled flights
        propCancelled = (double) sumCancelled / sumTotal;
        
        // Emit pair (airport, percentage of cancelled flights)
        if (propCancelled >= 0.01)
            context.write(key, new DoubleWritable(propCancelled));
    }

}