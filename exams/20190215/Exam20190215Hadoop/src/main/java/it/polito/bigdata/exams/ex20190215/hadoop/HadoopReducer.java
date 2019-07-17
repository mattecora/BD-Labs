package it.polito.bigdata.exams.ex20190215.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        Integer tourismPOIs = 0, museumPOIs = 0;

        // Loop through the values
        for (Text subcat : values) {
            // Increment tourism POIs
            tourismPOIs++;

            // Check and increment museum POIs
            if (subcat.toString().equals("Museum"))
                museumPOIs++;
        }

        // Emit the pair if constraints satisfied
        if (tourismPOIs >= 1000 && museumPOIs >= 20)
            context.write(key, NullWritable.get());
    }

}