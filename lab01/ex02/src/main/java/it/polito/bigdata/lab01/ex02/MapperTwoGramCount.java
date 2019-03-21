package it.polito.bigdata.lab01.ex02;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperTwoGramCount extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        IntWritable> {// Output value type

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // Split each sentence in words. Use whitespace(s) as delimiter
        String[] words = value.toString().split("\\s+");

        // Iterate over the set of words
        for (int i = 0; i < words.length - 1; i++) {
            // Transform word case
            String firstWord = words[i].toLowerCase();
            String secondWord = words[i+1].toLowerCase();

            // Emit the pair (word, 1)
            context.write(new Text(firstWord + ";" + secondWord), new IntWritable(1));
        }
    }
}