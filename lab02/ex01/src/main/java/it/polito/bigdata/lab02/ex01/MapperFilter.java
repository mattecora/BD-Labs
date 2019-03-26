package it.polito.bigdata.lab02.ex01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperFilter extends Mapper<Text, Text, Text, IntWritable> {

    private String beginningString;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        beginningString = context.getConfiguration().get("beginningString");
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        if (key.toString().startsWith(beginningString)) {
            context.write(key, new IntWritable(Integer.parseInt(value.toString())));
        }
    }

}
