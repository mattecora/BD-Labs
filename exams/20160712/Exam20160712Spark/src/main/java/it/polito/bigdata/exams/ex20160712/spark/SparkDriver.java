package it.polito.bigdata.exams.ex20160712.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

    public static void main(String[] args) {
        String inputFile1 = args[0], inputFile2 = args[1];
        String outputFolder1 = args[2], outputFolder2 = args[3];

        // Create the Spark context
        SparkConf conf = new SparkConf().setAppName("Exam 20160712 - Spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the input files
        JavaRDD<String> occupancy = sc.textFile(inputFile1).cache();
        JavaRDD<String> stations = sc.textFile(inputFile2).cache();

        /*
         * TASK A
         */

        JavaRDD<String> smallStations = stations
            .filter(s -> Integer.parseInt(s.split(",")[4]) < 5)                 // Select only stations with < 5 places
            .map(s -> s.split(",")[0]);                                         // Keep only the station ID

        JavaRDD<String> potentiallyCriticalSmallStations = occupancy
            .filter(s -> Integer.parseInt(s.split(",")[5]) == 0)                // Select only readings with no free slot
            .map(s -> s.split(",")[0])                                          // Keep only the station ID
            .intersection(smallStations);                                       // Intersect with the small stations

        potentiallyCriticalSmallStations.saveAsTextFile(outputFolder1);
        
        /*
         * TASK B
         */
        
        JavaRDD<String> wellSizedStations = occupancy
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                Integer value = Integer.parseInt(tokens[5]) < 3 ? 1 : 0;

                return new Tuple2<>(tokens[0], value);
            })                                                                  // Create pairs (station, less than 3 slots)
            .reduceByKey((v1, v2) -> v1 + v2)                                   // Reduce to (station, number of less than 3 slots)
            .filter(p -> p._2() == 0)                                           // Remove if at least one reading has less than 3 slots
            .keys();                                                            // Keep only the station ID

        wellSizedStations.saveAsTextFile(outputFolder2);

        // Close the Spark context
        sc.close();
    }

}