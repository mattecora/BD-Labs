package it.polito.bigdata.exams.ex20190702.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        // Split the input line
        String[] tokens = value.toString().split(",");

        // Check if the city is Italian
        if (tokens[3].equals("Italy")) {
            // Emit a pair (city, producer)
            context.write(new Text(tokens[2]), new Text(tokens[1]));
        }
    }

}