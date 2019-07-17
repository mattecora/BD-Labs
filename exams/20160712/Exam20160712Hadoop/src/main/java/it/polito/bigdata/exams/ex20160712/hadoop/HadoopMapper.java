package it.polito.bigdata.exams.ex20160712.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // Split the input line
        String[] tokens = value.toString().split(",");
        
        // Check that city is Turin and slots at least 20
        if (tokens[3].equals("Turin") && Integer.parseInt(tokens[4]) >= 20) {
            // Emit a pair (zone, 1)
            context.write(new Text(tokens[2]), new IntWritable(1));
        }
    }

}