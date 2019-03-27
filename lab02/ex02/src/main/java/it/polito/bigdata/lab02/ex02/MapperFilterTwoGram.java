package it.polito.bigdata.lab02.ex02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperFilterTwoGram extends Mapper<Text, Text, Text, IntWritable> {

    private String beginningString;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        beginningString = context.getConfiguration().get("beginningString");
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = key.toString().split(";");

        if (tokens[0].startsWith(beginningString) || tokens[1].startsWith(beginningString)) {
            context.write(key, new IntWritable(Integer.parseInt(value.toString())));
        }
    }

}