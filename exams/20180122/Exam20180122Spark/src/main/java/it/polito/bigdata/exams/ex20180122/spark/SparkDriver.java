package it.polito.bigdata.exams.ex20180122.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

    public static void main(String[] args) {
        String inputFile1 = args[0], inputFile2 = args[1];
        String outputFolder1 = args[2], outputFolder2 = args[3];

        // Create the Spark context
        SparkConf conf = new SparkConf().setAppName("Exam 20180122 - Spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the input files
        JavaRDD<String> booksRDD = sc.textFile(inputFile1).cache();
        JavaRDD<String> salesIn2017RDD = sc.textFile(inputFile2).filter(s -> s.startsWith("2017")).cache();

        /*
         * TASK A
         */

        JavaRDD<String> booksWithAnomalousBehavior = salesIn2017RDD
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                Double price = Double.parseDouble(tokens[3]);
                return new Tuple2<>(tokens[1], new MaxAndMin(price, price));
            })                                                                  // Create pairs (book, (price, price))
            .reduceByKey((v1, v2) -> new MaxAndMin(
                v1.getMax() > v2.getMax() ? v1.getMax() : v2.getMax(),
                v1.getMin() < v2.getMin() ? v1.getMin() : v2.getMin()
            ))                                                                  // Reduce to (book, (max, min))
            .filter(p -> p._2().getMax() - p._2().getMin() > 15)                // Remove if max - min < 15
            .map(p -> p._1() + "," + p._2.getMax() + "," + p._2().getMin());    // Map to the requested format

        booksWithAnomalousBehavior.saveAsTextFile(outputFolder1);

        /*
         * TASK B
         */
        
        // List the books not purchased in 2017
        List<String> booksNotPurchased = booksRDD
            .map(s -> s.split(",")[0])                                          // Map on the book ID
            .subtract(salesIn2017RDD.map(s -> s.split(",")[1]))                 // Remove books sold at least once
            .collect();                                                         // Collect results in a list

        JavaRDD<String> bookGenresWithNotPurchasedProp = booksRDD
            .mapToPair(s -> {
                String[] tokens = s.split(",");

                // Check if the book was not purchased
                return new Tuple2<>(tokens[2], new SumAndCount(booksNotPurchased.contains(tokens[0]) ? 1 : 0, 1));
            })                                                                          // Create pairs (genre, (0/1, 1))
            .reduceByKey((v1, v2) -> new SumAndCount(
                v1.getSum() + v2.getSum(),
                v1.getCount() + v2.getCount()
            ))                                                                          // Reduce to (genre, (sum, count))
            .map(p -> p._1() + "," + ((double) p._2().getSum() /p._2().getCount()));    // Map to requested format

        bookGenresWithNotPurchasedProp.saveAsTextFile(outputFolder2);

        // Close the Spark context
        sc.close();
    }

}