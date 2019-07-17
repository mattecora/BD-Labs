package it.polito.bigdata.exams.ex20160701.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
            throws IOException, InterruptedException {
        // Split the input line
        String[] tokens = value.toString().split(",");
        
        // Emit a pair (bookid, price)
        context.write(new Text(tokens[1]), new DoubleWritable(Double.parseDouble(tokens[3])));
    }

}