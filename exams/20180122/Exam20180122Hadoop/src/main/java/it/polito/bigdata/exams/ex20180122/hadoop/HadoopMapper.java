package it.polito.bigdata.exams.ex20180122.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        // Split the input line
        String[] tokens = value.toString().split(",");
        
        // Check if year 2016
        if (tokens[2].startsWith("2016")) {
            // Emit a pair (book, 1)
            context.write(new Text(tokens[1]), new LongWritable(1));
        }
    }

}