package it.polito.bigdata.lab04.ex01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperItemsRating1 extends Mapper<LongWritable, Text, Text, ItemRatingWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the first line
        if (key.get() == 0)
            return;

        // Split the input string
        String[] tokens = value.toString().split(",");

        // Emit a pair (user, item+rating)
        context.write(new Text(tokens[2]), new ItemRatingWritable(tokens[1], Integer.parseInt(tokens[6])));
    }
}
