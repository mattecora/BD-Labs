package it.polito.bigdata.exams.ex20190215.hadoop;

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

        // Check if the city is Italian and if the category is Tourism
        if (tokens[4].equals("Italy") && tokens[5].equals("Tourism")) {
            // Emit a pair (city, subcategory)
            context.write(new Text(tokens[3]), new Text(tokens[6]));
        }
    }

}