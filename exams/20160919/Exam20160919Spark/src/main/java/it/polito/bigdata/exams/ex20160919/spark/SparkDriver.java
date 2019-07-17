package it.polito.bigdata.exams.ex20160919.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFolder1 = args[1], outputFolder2 = args[2];
        Double PM10Limit = Double.parseDouble(args[3]), PM25Limit = Double.parseDouble(args[4]);

        // Create the Spark context
        SparkConf conf = new SparkConf().setAppName("Exam 20160919 - Spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the input files
        JavaRDD<String> readings = sc.textFile(inputFile).cache();

        /*
         * TASK A
         */

        JavaRDD<String> highlyPollutedStations = readings
            .flatMapToPair(s -> {
                List<Tuple2<String, Integer>> returnedValues = new ArrayList<>();
                String[] tokens = s.split(",");

                // Check that the year is 2015 and the PM10 is high
                if (tokens[1].startsWith("2015") && Double.parseDouble(tokens[5]) > PM10Limit) {
                    // Emit a pair (station, 1)
                    returnedValues.add(new Tuple2<>(tokens[0], 1));
                }

                return returnedValues.iterator();
            })
            .reduceByKey((v1, v2) -> v1 + v2)               // Reduce to (station, total readings)
            .filter(p -> p._2() >= 45)                      // Select only when at least 45 readings present
            .keys();                                        // Keep only the station part
        
        highlyPollutedStations.saveAsTextFile(outputFolder1);
        
        /*
         * TASK B
         */
        
        JavaRDD<String> alwaysPollutedStations = readings
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                Integer value = Double.parseDouble(tokens[6]) <= PM25Limit ? 1 : 0;

                return new Tuple2<>(tokens[0], value);
            })                                              // Create pairs (station, PM25 less than limit)
            .reduceByKey((v1, v2) -> v1 + v2)               // Reduce to (station, number of PM25 less than limit)
            .filter(p -> p._2() == 0)                       // Remove if at least one reading is lower than limit
            .keys();                                        // Keep only the station ID

        alwaysPollutedStations.saveAsTextFile(outputFolder2);

        // Close the Spark context
        sc.close();
    }

}