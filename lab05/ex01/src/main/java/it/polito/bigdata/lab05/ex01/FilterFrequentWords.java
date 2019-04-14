package it.polito.bigdata.lab05.ex01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FilterFrequentWords {

    public static void main(String[] args) {
        // Parse command-line parameters
        String inputPath = args[0];
        String outputPath = args[1];
        String begin = args[2];

        // Create the Spark config
        SparkConf conf = new SparkConf().setAppName("Lab 5.1 - Frequents words filtering");

        // Create the Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create the input RDD
        JavaRDD<String> input = sc.textFile(inputPath);

        // Perform the filtering operation and cache results
        JavaRDD<String> filteredInput = input
                .filter(s -> s.startsWith(begin))
                .cache();

        // Count the number of filtered records
        long linesNum = filteredInput.count();
        System.out.println("Number of filtered lines: " + linesNum);

        // Compute the maximum frequency
        long maxFreq = filteredInput
                .map(s -> Long.parseLong(s.split("\t")[1]))
                .reduce((f1, f2) -> (f1 > f2) ? f1 : f2);
        System.out.println("Maximum frequency: " + maxFreq);

        // Select the frequent words and cache results
        JavaRDD<String> frequentWords = filteredInput
                .filter(s -> {
                    long freq = Long.parseLong(s.split("\t")[1]);
                    return (freq > 0.8 * maxFreq);
                }).cache();

        // Count the frequent words
        long frequentNum = frequentWords.count();
        System.out.println("Number of frequent words: " + frequentNum);

        // Store the frequent words in the output folder
        frequentWords
                .map(s -> s.split("\t")[0])
                .saveAsTextFile(outputPath);

        // Close the Spark context
        sc.close();
    }

}
