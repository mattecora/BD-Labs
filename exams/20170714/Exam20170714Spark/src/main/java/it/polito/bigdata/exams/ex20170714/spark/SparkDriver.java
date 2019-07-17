package it.polito.bigdata.exams.ex20170714.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFolder1 = args[1], outputFolder2 = args[2];

        // Create the Spark context
        SparkConf conf = new SparkConf().setAppName("Exam 20170714 - Spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the input files
        JavaRDD<String> readingsFromSummer2015 = sc
            .textFile(inputFile)
            .filter(s -> s.split(",")[0].compareTo("2015/06/01") >= 0 && s.split(",")[0].compareTo("2015/08/31") <= 0)
            .cache();

        /*
         * TASK A
         */

        JavaPairRDD<String, Double> avgMaxTempPerCity = readingsFromSummer2015
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                return new Tuple2<>(tokens[1] + "-" + tokens[2], new Average(Double.parseDouble(tokens[3]), 1));
            })
            .reduceByKey((v1, v2) -> new Average(v1.getSum() + v2.getSum(), v1.getCount() + v2.getCount()))
            .mapValues(v -> v.getSum() / v.getCount())
            .cache();
        
        avgMaxTempPerCity.saveAsTextFile(outputFolder1);
        
        /*
         * TASK B
         */
        
        // Compute average maximum temperature per country
        JavaPairRDD<String, Double> avgMaxTempPerCountry = readingsFromSummer2015
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                return new Tuple2<>(tokens[2], new Average(Double.parseDouble(tokens[3]), 1));
            })
            .reduceByKey((v1, v2) -> new Average(v1.getSum() + v2.getSum(), v1.getCount() + v2.getCount()))
            .mapValues(v -> v.getSum() / v.getCount());

        // Get hot cities
        JavaRDD<String> hotCities = avgMaxTempPerCity
            .mapToPair(p -> new Tuple2<>(p._1().split("-")[1], new Tuple2<>(p._1().split("-")[0], p._2())))
            .join(avgMaxTempPerCountry)
            .filter(p -> p._2()._1()._2() >= p._2()._2() + 5.0)
            .map(p -> p._2()._1()._1());
        
        hotCities.saveAsTextFile(outputFolder2);

        // Close the Spark context
        sc.close();
    }

}