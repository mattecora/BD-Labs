package it.polito.bigdata.lab03.ex01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperBoughtTogether2 extends Mapper<Text, Text, Text, IntWritable> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // Emit the input pair by extracting the value
        context.write(key, new IntWritable(Integer.parseInt(value.toString())));
    }

}
