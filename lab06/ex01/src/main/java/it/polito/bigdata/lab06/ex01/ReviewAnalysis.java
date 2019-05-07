package it.polito.bigdata.lab06.ex01;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ReviewAnalysis {

    public static void main(String[] args) {
        // Parse command-line parameters
        String inputPath = args[0];
        String outputPath = args[1];

        // Create the Spark config
        SparkConf conf = new SparkConf().setAppName("Lab 6.1 - Analysis of the Amazon reviews dataset");

        // Create the Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create the input RDD
        JavaRDD<String> input = sc.textFile(inputPath);

        // Transpose the dataset
        JavaPairRDD<String, Iterable<String>> pairs = input
            .filter(s -> !s.startsWith("Id"))
            .mapToPair(s -> new Tuple2<>(s.split(",")[2], s.split(",")[1]))
            .groupByKey();
        
        // Count the frequencies of pairs of products
        JavaPairRDD<String, Integer> frequencies = pairs
            .values()
            .flatMapToPair(l -> {
                List<Tuple2<String, Integer>> productPairs = new ArrayList<>();

                for (String p1 : l) {
                    for (String p2 : l) {
                        if (p1 != p2 && !productPairs.contains(new Tuple2<>(p2 + "," + p1, 1)))
                            productPairs.add(new Tuple2<>(p1 + "," + p2, 1));
                    }
                }

                return productPairs.iterator();
            })
            .reduceByKey((v1, v2) -> v1 + v2);
        
        // Filter the frequent pairs and sort
        JavaPairRDD<Integer, String> filteredFrequencies = frequencies
            .filter(p -> p._2() > 1)
            .mapToPair(p -> new Tuple2<>(p._2(), p._1()))
            .sortByKey(false);
        
        // Store results in the output folder
        filteredFrequencies.saveAsTextFile(outputPath);

        // Close the Spark context
        sc.close();
    }

}
