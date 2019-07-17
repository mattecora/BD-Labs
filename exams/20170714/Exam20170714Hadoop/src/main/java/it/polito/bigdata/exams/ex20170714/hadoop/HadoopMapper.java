package it.polito.bigdata.exams.ex20170714.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static int HIGH = 1;
    private final static int LOW = 2;

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // Split the input line
        String[] tokens = value.toString().split(",");
        
        // Emit a pair (city, HIGH) if highTemp > 35
        if (Double.parseDouble(tokens[3]) > 35.0)
            context.write(new Text(tokens[1]), new IntWritable(HIGH));
        
        // Emit a pair (city, LOW) if lowTemp < -20
        if (Double.parseDouble(tokens[4]) < -20.0)
            context.write(new Text(tokens[1]), new IntWritable(LOW));
    }

}