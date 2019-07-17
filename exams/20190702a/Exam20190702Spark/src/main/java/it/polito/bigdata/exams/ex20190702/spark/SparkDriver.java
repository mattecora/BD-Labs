package it.polito.bigdata.exams.ex20190702.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

    public static void main(String[] args) {
        String bicyclesFile = args[0], failuresFile = args[1];
        String outputFolder1 = args[2], outputFolder2 = args[3];

        // Create the Spark context
        SparkConf conf = new SparkConf().setAppName("Exam 20190702 - Spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the input files
        JavaRDD<String> bicyclesRDD = sc.textFile(bicyclesFile).cache();
        JavaRDD<String> failuresRDD = sc.textFile(failuresFile).cache();

        /*
         * TASK A
         */

        JavaRDD<String> bikesWithMoreThanTwoFailuresInAMonth = failuresRDD
            .flatMapToPair(s -> {
                List<Tuple2<BikeFailureMonth, Integer>> returnedTuples = new ArrayList<>();

                // Split line in fields
                String[] tokens = s.split(",");
                String[] dateAndTime = tokens[0].split("_");
                String[] splittedDate = dateAndTime[0].split("/");

                // Check that year is 2018 and failure is wheel
                if (tokens[2].equals("Wheel") && splittedDate[0].equals("2018")) {
                    // Emit a pair (bicycle + month, 1)
                    returnedTuples.add(new Tuple2<>(new BikeFailureMonth(tokens[1], splittedDate[1]), 1));
                }

                return returnedTuples.iterator();
            })
            .reduceByKey((v1, v2) -> v1 + v2)   // Sum failures per bike and month
            .filter(p -> p._2() > 2)            // Filter bikes having at least two failures
            .map(p -> p._1().getBike())         // Retain only the ID of the bike
            .distinct();                        // Remove duplicates
        
        bikesWithMoreThanTwoFailuresInAMonth.saveAsTextFile(outputFolder1);

        /*
         * TASK B
         */

        // Get the city for each bicycle
        JavaPairRDD<String, String> bicycleCity = bicyclesRDD
            .mapToPair(s -> new Tuple2<>(s.split(",")[0], s.split(",")[2]))
            .cache();

        // Count failures per city
        JavaRDD<String> citiesWithManyFailures = failuresRDD.flatMapToPair(s -> {
            List<Tuple2<String, Integer>> returnedTuples = new ArrayList<>();

            // Split line in fields
            String[] tokens = s.split(",");
            String[] dateAndTime = tokens[0].split("_");
            String[] splittedDate = dateAndTime[0].split("/");

            // Check that year is 2018
            if (splittedDate[0].equals("2018")) {
                // Emit a pair (bicycle, 1)
                returnedTuples.add(new Tuple2<>(tokens[1], 1));
            }

            return returnedTuples.iterator();
        })
        .reduceByKey((v1, v2) -> v1 + v2)           // Sum failures per bike: (bike, total failures)
        .join(bicycleCity)                          // Get the city of the bike: (bike, (total failures, city))
        .mapToPair(p -> new Tuple2<>(p._2()._2(), p._2()._1())) // Group by city: (city, total failures per bike)
        .groupByKey()                               // Group by city part 2: (city, list of failures per bike)
        .filter(p -> {
            // Select cities if at least one bike had 20 failures
            for (Integer fails : p._2()) {
                if (fails > 20)
                    return true;
            }
            return false;
        })
        .map(p -> p._1());                          // Retain only the name of the city

        // Get cities with few failures by subtraction
        JavaRDD<String> citiesWithFewFailures = bicycleCity
            .map(p -> p._2())                       // Keep only city name
            .distinct()                             // Remove duplicates
            .subtract(citiesWithManyFailures);      // Subtract cities with many failures

        citiesWithFewFailures.saveAsTextFile(outputFolder2);

        // Close the Spark context
        sc.close();
    }

}