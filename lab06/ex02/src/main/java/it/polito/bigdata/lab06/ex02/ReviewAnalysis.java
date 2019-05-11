package it.polito.bigdata.lab06.ex02;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ReviewAnalysis {

    private final static int K = 10;

    public static void main(String[] args) {
        // Parse command-line parameters
        String inputPath = args[0];

        // Create the Spark config
        SparkConf conf = new SparkConf().setAppName("Lab 6.2 - Analysis of the Amazon reviews dataset");

        // Create the Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create the input RDD
        JavaRDD<String> input = sc.textFile(inputPath);

        // Transpose the dataset
        JavaPairRDD<String, String> pairs = input
            .filter(s -> !s.startsWith("Id"))
            .mapToPair(s -> new Tuple2<>(s.split(",")[2], s.split(",")[1]))
            .reduceByKey((p1, p2) -> p1 + "," + p2);
        
        // Count the frequencies of pairs of products
        JavaPairRDD<String, Integer> frequencies = pairs
            .values()
            .flatMapToPair(l -> {
                String[] tokens = l.split(",");
                List<Tuple2<String, Integer>> productPairs = new ArrayList<>();

                for (int i = 0; i < tokens.length; i++) {
                    for (int j = i + 1; j < tokens.length; j++) {
                        Tuple2<String, Integer> pair = new Tuple2<>(tokens[i] + "," + tokens[j], 1);
                        if (!productPairs.contains(pair))
                            productPairs.add(pair);
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
        
        // Extract the top-K
        List<Tuple2<Integer, String>> topK = filteredFrequencies.take(K);
        
        // Print the top-K
        for (Tuple2<Integer, String> t : topK) {
            System.out.println(t);
        }

        // Close the Spark context
        sc.close();
    }

}
