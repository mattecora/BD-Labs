package it.polito.bigdata.lab03.ex01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerBoughtTogether2 extends Reducer<Text, IntWritable, Text, IntWritable> {

    private TopKVector<WordCountWritable> topK;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Get K from the configuration object
        int K = context.getConfiguration().getInt("K", 100);

        // Create the top-K vector
        topK = new TopKVector<>(K);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Insert into the top-K
        topK.updateWithNewElement(new WordCountWritable(key.toString(), values.iterator().next().get()));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit the top-K
        for (WordCountWritable wc : topK.getLocalTopK()) {
            context.write(new Text(wc.getWord()), new IntWritable(wc.getCount()));
        }
    }

}
