package it.polito.bigdata.lab02.ex02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverFilterTwoGram extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Path inputPath;
        Path outputDir;
        int exitCode;

        // Parse the parameters
        inputPath = new Path(args[1]);
        outputDir = new Path(args[2]);

        Configuration conf = this.getConf();

        // Setup properties
        conf.set("beginningString", args[0]);

        // Define a new job
        Job job = Job.getInstance(conf);

        // Assign a name to the job
        job.setJobName("Lab 2.1 - Filter an input dataset");

        // Set path of the input file/folder for this job
        FileInputFormat.addInputPath(job, inputPath);

        // Set path of the output folder for this job
        FileOutputFormat.setOutputPath(job, outputDir);

        // Specify the class of the Driver for this job
        job.setJarByClass(DriverFilterTwoGram.class);

        // Set job input format
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // Set job output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set map class
        job.setMapperClass(MapperFilterTwoGram.class);

        // Set map output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set number of reducers
        job.setNumReduceTasks(0);

        // Execute the job and wait for completion
        if (job.waitForCompletion(true))
            exitCode=0;
        else
            exitCode=1;

        return exitCode;
    }

    public static void main(String args[]) throws Exception {
        // Exploit the ToolRunner class to "configure" and run the Hadoop application
        int res = ToolRunner.run(new Configuration(), new DriverFilterTwoGram(), args);

        System.exit(res);
    }

}