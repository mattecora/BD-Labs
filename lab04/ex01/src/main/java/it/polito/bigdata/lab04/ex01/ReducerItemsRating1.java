package it.polito.bigdata.lab04.ex01;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReducerItemsRating1 extends Reducer<Text, ItemRatingWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<ItemRatingWritable> values, Context context) throws IOException, InterruptedException {
        int n = 0;
        double userAvg = 0;
        List<ItemRatingWritable> valueList = new ArrayList<>();

        // Compute the average value for the user
        for (ItemRatingWritable ir : values) {
            n++;
            userAvg = userAvg + ir.getRating();
            valueList.add(new ItemRatingWritable(ir.getItem(), ir.getRating()));
        }
        userAvg = userAvg / n;

        // Emit a pair (item, normalized rating)
        for (ItemRatingWritable ir : valueList) {
            context.write(new Text(ir.getItem()), new DoubleWritable(ir.getRating() - userAvg));
        }
    }
}
