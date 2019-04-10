package it.polito.bigdata.lab04.ex01;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperItemsRating2 extends Mapper<Text, Text, Text, DoubleWritable> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // Emit a pair (item, normalized rating)
        context.write(key, new DoubleWritable(Double.parseDouble(value.toString())));
    }
}
