package it.polito.bigdata.lab03.ex01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class MapperBoughtTogether extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input key on commas
        String[] tokens = value.toString().split(",");

        // Sort tokens alphabetically
        Arrays.sort(tokens);

        // For each couple of item IDs, emit a ("word1,word2", 1) pair
        for (int i = 1; i < tokens.length; i++) {
            for (int j = i; j < tokens.length; j++) {
                if (i != j) {
                    context.write(new Text(tokens[i] + "," + tokens[j]), new IntWritable(1));
                }
            }
        }
    }

}
