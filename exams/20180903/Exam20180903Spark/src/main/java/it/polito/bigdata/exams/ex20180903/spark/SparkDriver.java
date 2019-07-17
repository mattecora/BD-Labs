package it.polito.bigdata.exams.ex20180903.spark;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

    final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd");

    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFolder1 = args[1], outputFolder2 = args[2];

        // Create the Spark context
        SparkConf conf = new SparkConf().setAppName("Exam 20180903 - Spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the input file
        JavaRDD<String> stocks2017RDD = sc
            .textFile(inputFile)
            .filter(s -> s.split(",")[1].startsWith("2017"))
            .cache();

        /*
         * TASK A
         */

        JavaPairRDD<StockAndDate, Double> dailyVariationPerStockAndDate = stocks2017RDD
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                return new Tuple2<>(new StockAndDate(tokens[0], dateFormatter.parse(tokens[1])), Double.parseDouble(tokens[3]));
            })                              // Create pairs (stock + date, price)
            .groupByKey()                   // Group values for the same stock and day
            .mapValues(v -> {
                Double max = Double.MIN_VALUE, min = Double.MAX_VALUE;

                for (Double val : v) {
                    if (val < min) min = val;
                    if (val > max) max = val;
                }

                return max - min;
            })                              // Map values to the difference max - min
            .cache();

        JavaRDD<String> stocksWithHighDailyPriceVariation = dailyVariationPerStockAndDate
            .filter(p -> p._2() > 10)       // Select stocks and dates with variation more than 10
            .map(p -> p._1().getStock())    // Map to the name of the stock
            .distinct();                    // Get distinct values
        
        stocksWithHighDailyPriceVariation.saveAsTextFile(outputFolder1);

        /*
         * TASK B
         */

        JavaRDD<String> stocksWithStableTrends = dailyVariationPerStockAndDate
            .flatMapToPair(p -> {
                List<Tuple2<StockAndDate, Double>> returnedValues = new ArrayList<>();

                // Get the yesterday's date
                Calendar cal = Calendar.getInstance();
                cal.setTime(p._1().getDate());
                cal.add(Calendar.DATE, -1);

                // Create two tuples, one with the current date and one with yesterday's
                returnedValues.add(p);
                returnedValues.add(new Tuple2<>(new StockAndDate(p._1().getStock(), cal.getTime()), p._2()));

                return returnedValues.iterator();
            })
            .reduceByKey((v1, v2) -> Math.abs(v1 - v2))     // Compute the difference between the daily variations
            .filter(p -> p._2() <= 0.1)                     // Select pairs with difference less than 0.1
            .map(p -> p._1().getStock() + "," + dateFormatter.format(p._1().getDate()));    // Map to given format
        
        stocksWithStableTrends.saveAsTextFile(outputFolder2);

        // Close the Spark context
        sc.close();
    }

}