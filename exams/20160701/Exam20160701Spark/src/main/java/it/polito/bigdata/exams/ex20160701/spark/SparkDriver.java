package it.polito.bigdata.exams.ex20160701.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

    public static void main(String[] args) {
        String inputFile1 = args[0], inputFile2 = args[1];
        String outputFolder1 = args[2], outputFolder2 = args[3];
        Double threshold = Double.parseDouble(args[4]);

        // Create the Spark context
        SparkConf conf = new SparkConf().setAppName("Exam 20160701 - Spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the input files
        JavaRDD<String> purchases = sc.textFile(inputFile1).cache();
        JavaRDD<String> books = sc.textFile(inputFile2).cache();

        /*
         * TASK A
         */

        JavaRDD<String> soldBooks = purchases.map(s -> s.split(",")[1]);        // Books sold at least once

        JavaRDD<String> expensiveNeverSoldBooks = books
            .filter(s -> Double.parseDouble(s.split(",")[3]) > 30)              // Filter that price > 30
            .map(s -> s.split(",")[0])                                          // Map on the book ID
            .subtract(soldBooks);                                               // Remove books sold at least once
        
        expensiveNeverSoldBooks.saveAsTextFile(outputFolder1);
        
        /*
         * TASK B
         */
        
        JavaRDD<String> cheapPurchasesCustomers = purchases
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                Integer value = Double.parseDouble(tokens[3]) < 10 ? 1 : 0;

                return new Tuple2<>(tokens[0], new SumAndCount(value, 1));
            })                                                                      // Create pairs (customer, (<10, 1))
            .reduceByKey((v1, v2) -> new SumAndCount(
                v1.getSum() + v2.getSum(),
                v1.getCount() + v2.getCount()
            ))                                                                      // Reduce to (customer, (tot <10, tot))
            .filter(p -> (double) p._2().getSum() / p._2().getCount() > threshold)  // Filter customers with % > threshold
            .keys();                                                                // Keep only the customer ID
        
        cheapPurchasesCustomers.saveAsTextFile(outputFolder2);

        // Close the Spark context
        sc.close();
    }

}