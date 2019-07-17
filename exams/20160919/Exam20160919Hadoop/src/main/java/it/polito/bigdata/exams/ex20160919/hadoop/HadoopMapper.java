package it.polito.bigdata.exams.ex20160919.hadoop;

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

        // Read PM10 and PM25
        Double PM10 = Double.parseDouble(tokens[4]);
        Double PM25 = Double.parseDouble(tokens[5]);
        
        // Check that year is 2013 and PM10 > PM25
        if (tokens[1].startsWith("2013") && PM10 > PM25) {
            // Emit a pair (station, 1)
            context.write(new Text(tokens[0]), new IntWritable(1));
        }
    }

}