package it.polito.bigdata.lab03.ex01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverBoughtTogether extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Path inputPath;
        Path outputDir1, outputDir2;
        int numberOfReducers;
        int exitCode;

        // Parse the parameters
        numberOfReducers = Integer.parseInt(args[0]);
        inputPath = new Path(args[2]);
        outputDir1 = new Path(args[3] + "_1");
        outputDir2 = new Path(args[3] + "_2");

        Configuration conf = this.getConf();

        // Set K value from the command line
        conf.set("K", args[1]);

        // Define a new job
        Job job = Job.getInstance(conf);

        // Assign a name to the job
        job.setJobName("Lab 3.1 - Items frequently bought together (job 1)");

        // Set path of the input file/folder for this job
        FileInputFormat.addInputPath(job, inputPath);

        // Set path of the output folder for this job
        FileOutputFormat.setOutputPath(job, outputDir1);

        // Specify the class of the Driver for this job
        job.setJarByClass(DriverBoughtTogether.class);

        // Set job input format
        job.setInputFormatClass(TextInputFormat.class);

        // Set job output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set map class
        job.setMapperClass(MapperBoughtTogether1.class);

        // Set map output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set reduce class
        job.setReducerClass(ReducerBoughtTogether1.class);

        // Set reduce output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set number of reducers
        job.setNumReduceTasks(numberOfReducers);

        // Execute the job and wait for completion
        if (job.waitForCompletion(true)) {
            // Define a new job
            Job job2 = Job.getInstance(conf);

            // Assign a name to the job
            job2.setJobName("Lab 3.1 - Items frequently bought together (job 2)");

            // Set path of the input file/folder for this job
            FileInputFormat.addInputPath(job2, outputDir1);

            // Set path of the output folder for this job
            FileOutputFormat.setOutputPath(job2, outputDir2);

            // Specify the class of the Driver for this job
            job2.setJarByClass(DriverBoughtTogether.class);

            // Set job input format
            job2.setInputFormatClass(KeyValueTextInputFormat.class);

            // Set job output format
            job2.setOutputFormatClass(TextOutputFormat.class);

            // Set map class
            job2.setMapperClass(MapperBoughtTogether2.class);

            // Set map output key and value classes
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IntWritable.class);

            // Set reduce class
            job2.setReducerClass(ReducerBoughtTogether2.class);

            // Set reduce output key and value classes
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            // Set number of reducers
            job2.setNumReduceTasks(1);

            if (job2.waitForCompletion(true))
                exitCode=0;
            else
                exitCode=1;
        }
        else
            exitCode=1;

        return exitCode;
    }

    public static void main(String args[]) throws Exception {
        // Exploit the ToolRunner class to "configure" and run the Hadoop application
        int res = ToolRunner.run(new Configuration(), new DriverBoughtTogether(), args);

        System.exit(res);
    }

}