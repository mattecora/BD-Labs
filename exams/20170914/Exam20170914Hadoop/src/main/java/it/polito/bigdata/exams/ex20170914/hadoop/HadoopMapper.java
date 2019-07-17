package it.polito.bigdata.exams.ex20170914.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopMapper extends Mapper<LongWritable, Text, Text, BooleanWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, BooleanWritable>.Context context)
            throws IOException, InterruptedException {
        // Split the input line
        String[] tokens = value.toString().split(",");
        
        // Check if between 2016/09/01 and 2017/08/31
        if (tokens[2].compareTo("2016/09/01") >= 0 && tokens[2].compareTo("2017/08/31") <= 0) {
            // Emit a pair (airport, cancelled true/false)
            context.write(new Text(tokens[5]), tokens[8].equals("yes") ? new BooleanWritable(true) : new BooleanWritable(false));
        }
    }

}