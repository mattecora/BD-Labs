package it.polito.bigdata.exams.ex20180626.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        // Split the input line
        String[] tokens = value.toString().split(",");
        
        // Check if May 2018, correct timeslot and CPU usage > 99.8
        if (tokens[0].startsWith("2018/05") &&
            tokens[1].compareTo("08:00") >= 0 && tokens[1].compareTo("17:59") <= 0 &&
            Double.parseDouble(tokens[3]) >= 99.8) {
            context.write(new Text(tokens[2]), NullWritable.get());
        }
    }

}