package it.polito.bigdata.exams.ex20180903.hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<NullWritable, Text, Text, Text> {

    Double globalTopValue = null;
    Date globalTopDate = null;

    final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd,HH:mm");

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Reducer<NullWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        // Loop through the values
        for (Text value : values) {
            // Split the input line
            String[] tokens = value.toString().split("\t");
            Date date = null;
            Double stockValue = null;

            try {
                // Read the value
                stockValue = Double.parseDouble(tokens[0]);
                
                // Read the date
                date = dateFormatter.parse(tokens[1]);
            } catch (Exception e) {
                // Malformed record, skip it
                return;
            }

            // Update local top
            if (globalTopValue == null || stockValue > globalTopValue || (stockValue == globalTopValue && date.before(globalTopDate))) {
                globalTopValue = stockValue;
                globalTopDate = date;
            }
        }

        // Emit the single local top value
        context.write(new Text(globalTopValue.toString()), new Text(dateFormatter.format(globalTopDate)));
    }

}