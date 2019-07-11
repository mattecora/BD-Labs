package it.polito.bigdata.lab10.ex01;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class HashtagCount {

    public static void main(String[] args) throws InterruptedException {
        // Parse command-line parameters
        String inputPath = args[0];
        String outputPath = args[1];

        // Create the Spark configuration
        SparkConf conf = new SparkConf().setAppName("Lab 10.1 - Hashtag count with Spark Streaming");
        
        // Open the streaming context
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(10));

        // Create the input DStream
        JavaDStream<String> inputStream = sc.textFileStream(inputPath);
        
        // Configure the window size
        JavaDStream<String> windowedInputStream = inputStream.window(Durations.seconds(30), Durations.seconds(10));

        // Extract the hashtags
        JavaDStream<String> hashtagsStream = windowedInputStream.flatMap(s -> {
            List<String> hashtags = new ArrayList<>();
            
            // Split the string in words
            String[] words = s.split("\t")[1].split("\\s+");

            // Loop through the words to search for hashtags
            for (String word : words) {
                if (word.startsWith("#")) {
                    hashtags.add(word);
                    System.out.println(word);
                }
            }

            return hashtags.iterator();
        });

        // Count number of hashtags
        JavaPairDStream<String, Integer> hashtagsCountStream = hashtagsStream
            .mapToPair(h -> new Tuple2<>(h, 1))
            .reduceByKey((v1, v2) -> v1 + v2);
        
        // Sort hashtags by occurrances
        JavaPairDStream<Integer, String> sortedHashtagsCountStream = hashtagsCountStream
            .mapToPair(p -> new Tuple2<>(p._2(), p._1()))
            .transformToPair(rdd -> rdd.sortByKey())
            .cache();
        
        // Store results in the output folder
        sortedHashtagsCountStream.dstream().saveAsTextFiles(outputPath, "");

        // Print the first 10 hashtags
        sortedHashtagsCountStream.print(10);

        // Start the analysis and set timeout
        sc.start();
        sc.awaitTerminationOrTimeout(60000);

        // Close the streaming context
        sc.close();
    }
}
