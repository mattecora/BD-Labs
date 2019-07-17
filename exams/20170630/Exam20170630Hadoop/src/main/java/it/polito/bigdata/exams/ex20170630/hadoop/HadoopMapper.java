package it.polito.bigdata.exams.ex20170630.hadoop;

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
        
        // Check that year is 2016
        if (tokens[1].startsWith("2016")) {
            // Emit a pair (stock_month, price)
            context.write(new Text(tokens[0] + "_" + tokens[1].split("/")[1]), new DoubleWritable(Double.parseDouble(tokens[3])));
        }
    }

}