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
        String serversFile = args[0], anomaliesFile = args[1];
        String outputFolder1 = args[2], outputFolder2 = args[3];

        // Create the Spark context
        SparkConf conf = new SparkConf().setAppName("Exam 20190702 - Spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the input files
        JavaRDD<String> serversRDD = sc.textFile(serversFile).cache();
        JavaRDD<String> anomaliesRDD = sc.textFile(anomaliesFile).cache();

        /*
         * TASK A
         */

        JavaRDD<String> serversWithMoreThanFiftyAnomaliesInAYear = anomaliesRDD
            .flatMapToPair(s -> {
                List<Tuple2<ServerAnomalyYear, Integer>> returnedTuples = new ArrayList<>();

                // Split line in fields
                String[] tokens = s.split(",");
                String[] dateAndTime = tokens[1].split("_");
                String[] splittedDate = dateAndTime[0].split("/");
                Double temperature = Double.parseDouble(tokens[2]);

                // Check that year is from 2010 and temperature is at least 100
                if (Integer.parseInt(splittedDate[0]) >= 2010 && temperature >= 100) {
                    // Emit a pair (server + year, 1)
                    returnedTuples.add(new Tuple2<>(new ServerAnomalyYear(tokens[0], splittedDate[0]), 1));
                }

                return returnedTuples.iterator();
            })
            .reduceByKey((v1, v2) -> v1 + v2)   // Sum anomalies per server and year
            .filter(p -> p._2() > 50)           // Filter servers having at least 50 anomalies
            .map(p -> p._1().getServer())       // Retain only the ID of the server
            .distinct();                        // Remove duplicates
        
            serversWithMoreThanFiftyAnomaliesInAYear.saveAsTextFile(outputFolder1);

        /*
         * TASK B
         */

        // Get the data center for each server
        JavaPairRDD<String, String> serverDataCenter = serversRDD
            .mapToPair(s -> new Tuple2<>(s.split(",")[0], s.split(",")[2]))
            .cache();

        // Count anomalies per data center
        JavaRDD<String> dataCentersWithManyAnomalies = anomaliesRDD.flatMapToPair(s -> {
            List<Tuple2<String, Integer>> returnedTuples = new ArrayList<>();

            // Split line in fields
            String[] tokens = s.split(",");

            // Check that year is from 2010
            if (Integer.parseInt(tokens[0]) >= 2010) {
                // Emit a pair (bicycle, 1)
                returnedTuples.add(new Tuple2<>(tokens[1], 1));
            }

            return returnedTuples.iterator();
        })
        .reduceByKey((v1, v2) -> v1 + v2)           // Sum failures per server: (server, total anomalies)
        .join(serverDataCenter)                     // Get the data center of the server: (server, (total anomalies, data center))
        .mapToPair(p -> new Tuple2<>(p._2()._2(), p._2()._1())) // Group by data center: (data center, total anomalies per server)
        .groupByKey()                               // Group by data center part 2: (data center, list of anomalies per server)
        .filter(p -> {
            // Select data centers if at least one server had 10 anomalies
            for (Integer fails : p._2()) {
                if (fails > 10)
                    return true;
            }
            return false;
        })
        .map(p -> p._1());                          // Retain only the name of the data center

        // Get data centers with few anomalies by subtraction
        JavaRDD<String> dataCentersWithFewAnomalies = serverDataCenter
            .map(p -> p._2())                           // Keep only data center name
            .distinct()                                 // Remove duplicates
            .subtract(dataCentersWithManyAnomalies);    // Subtract data centers with many failures

        dataCentersWithFewAnomalies.saveAsTextFile(outputFolder2);

        // Close the Spark context
        sc.close();
    }

}