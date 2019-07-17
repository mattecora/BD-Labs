package it.polito.bigdata.exams.ex20170630.spark;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFolder1 = args[1], outputFolder2 = args[2];
        Integer numWeeks = Integer.parseInt(args[3]);

        // Create the Spark context
        SparkConf conf = new SparkConf().setAppName("Exam 20170630 - Spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the input files
        JavaRDD<String> stocksOf2016 = sc.textFile(inputFile).filter(s -> s.split(",")[1].startsWith("2016")).cache();

        /*
         * TASK A
         */

        JavaPairRDD<String, Double> pricePerStockAndDay = stocksOf2016
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                return new Tuple2<>(tokens[0] + "_" + tokens[1], Double.parseDouble(tokens[3]));
            })                                                                  // Create pairs (stock_date, price)
            .cache();                                                           // Cache results

        JavaPairRDD<String, Double> lowestPricePerStockAndDay = pricePerStockAndDay
            .reduceByKey((v1, v2) -> v1 < v2 ? v1 : v2)                         // Get the minimum price per day
            .sortByKey(true);                                                   // Sort by ascending key

        lowestPricePerStockAndDay.saveAsTextFile(outputFolder1);
        
        /*
         * TASK B
         */
        
        JavaRDD<String> stocksWithFrequentPositiveTrends = pricePerStockAndDay
            .reduceByKey((v1, v2) -> v1 > v2 ? v1 : v2)                         // Get the maximum price per day
            .mapToPair(p -> {
                String[] splitKey = p._1().split("_");
                Date date = new SimpleDateFormat("yyyy/MM/dd").parse(splitKey[1]);
                Calendar cal = Calendar.getInstance();

                cal.setTime(date);
                Integer week = cal.getWeekYear();

                return new Tuple2<>(splitKey[0] + "_" + week, new WeekMaxAndMin(p._2(), p._2(), splitKey[1], splitKey[1]));
            })                                                                  // Create (stock_week, (max, max, day, day)) pairs
            .reduceByKey((v1, v2) -> {
                Double maxFirst, maxLast;
                String dayFirst, dayLast;

                // Check first date
                if (v1.getDayFirst().compareTo(v2.getDayFirst()) < 0) {
                    maxFirst = v1.getMaxFirst();
                    dayFirst = v1.getDayFirst();
                } else {
                    maxFirst = v2.getMaxFirst();
                    dayFirst = v2.getDayFirst();
                }

                // Check second date
                if (v1.getDayLast().compareTo(v2.getDayLast()) > 0) {
                    maxLast = v1.getMaxLast();
                    dayLast = v1.getDayLast();
                } else {
                    maxLast = v2.getMaxLast();
                    dayLast = v2.getDayLast();
                }

                return new WeekMaxAndMin(maxFirst, maxLast, dayFirst, dayLast);
            })                                                                  // Reduce to (stock_week, (maxFirst, maxLast, first, last))
            .mapValues(v -> v.getMaxLast() - v.getMaxFirst())                   // Map values to absolute difference
            .filter(p -> p._2() > 0)                                            // Select only positive trends
            .mapToPair(p -> new Tuple2<>(p._1().split("_")[0], 1))              // Create (stock, 1) pairs
            .reduceByKey((v1, v2) -> v1 + v2)                                   // Reduce to (stock, numPosTrends)
            .filter(p -> p._2() >= numWeeks)                                    // Select only when numPosTrends >= numWeeks
            .keys();                                                            // Keep only the stock

        stocksWithFrequentPositiveTrends.saveAsTextFile(outputFolder2);

        // Close the Spark context
        sc.close();
    }

}