package it.polito.bigdata.exams.ex20180716.hadoop;

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
        
        // Check if April 2016 and if RAM or hard drive
        if (tokens[0].startsWith("2016/04") && (tokens[3].equals("RAM") || tokens[3].equals("Hard_drive"))) {
            // Emit a (SID, type) pair
            context.write(new Text(tokens[2]), new Text(tokens[3]));
        }
    }

}