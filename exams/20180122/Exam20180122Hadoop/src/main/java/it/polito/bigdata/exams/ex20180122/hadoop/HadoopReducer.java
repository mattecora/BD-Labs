package it.polito.bigdata.exams.ex20180122.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    Text bestBook = null;
    LongWritable bestBookSales = null;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        Long count = 0L;

        // Sum values
        for (LongWritable n : values)
            count = count + n.get();

        // Update top book
        if (bestBook == null || bestBookSales.get() < count || (bestBookSales.get() == count && bestBook.compareTo(key) < 0)) {
            bestBook = key;
            bestBookSales = new LongWritable(count);
        }
    }

    @Override
    protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        // Emit the top-1
        context.write(bestBook, bestBookSales);
    }

}