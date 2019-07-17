package it.polito.bigdata.exams.ex20180903.hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    Double localTopValue = null;
    Date localTopDate = null;

    final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd,HH:mm");

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        // Split the input line
        String[] tokens = value.toString().split(",");
        Date date = null;
        Double stockValue = null;

        // Check if the stock is from Google and from 2017
        if (tokens[0].equals("GOOG") && tokens[1].startsWith("2017")) {
            try {
                // Read the value
                stockValue = Double.parseDouble(tokens[3]);
                
                // Read the date
                date = dateFormatter.parse(tokens[1] + "," + tokens[2]);
            } catch (Exception e) {
                // Malformed record, skip it
                return;
            }

            // Update local top
            if (localTopValue == null || stockValue > localTopValue || (stockValue == localTopValue && date.before(localTopDate))) {
                localTopValue = stockValue;
                localTopDate = date;
            }
        }
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        // Emit the single local top value
        context.write(NullWritable.get(), new Text(localTopValue + "\t" + dateFormatter.format(localTopDate)));
    }

}