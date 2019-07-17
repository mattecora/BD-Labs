package it.polito.bigdata.exams.ex20180716.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

    public static void main(String[] args) {
        String inputFile1 = args[0], inputFile2 = args[1];
        String outputFolder1 = args[2], outputFolder2 = args[3];

        // Create the Spark context
        SparkConf conf = new SparkConf().setAppName("Exam 20180716 - Spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the input file
        JavaRDD<String> serversRDD = sc.textFile(inputFile1).cache();
        JavaRDD<String> failuresRDD = sc.textFile(inputFile2).cache();

        /*
         * TASK A
         */

        JavaRDD<String> failuresIn2017 = failuresRDD
            .filter(s -> s.startsWith("2017"))                                  // Filter failures in 2017
            .cache();                                                           // Cache the results

        JavaPairRDD<String, Integer> countFailuresPerServer = failuresRDD
            .filter(s -> s.startsWith("2017"))                                  // Filter failures in 2017
            .mapToPair(s -> new Tuple2<>(s.split(",")[2], 1))                   // Generate (server, 1) pairs
            .reduceByKey((v1, v2) -> v1 + v2);                                  // Reduce to (server, tot failures) pairs
        
        JavaRDD<String> dataCentersWithManyFailures = serversRDD
            .mapToPair(s -> new Tuple2<>(s.split(",")[0], s.split(",")[2]))     // Generate (server, data center) pairs
            .join(countFailuresPerServer)                                       // Join servers with their failures
            .mapToPair(p -> new Tuple2<>(p._2()._1(), p._2()._2()))             // Generate (data center, failures per server) pairs
            .reduceByKey((v1, v2) -> v1 + v2)                                   // Reduce to (data center, tot failures) pairs
            .filter(p -> p._2() >= 365)                                         // Select data centers with at least 365 failures
            .map(p -> p._1() + "," + p._2());                                   // Map to the correct format

        dataCentersWithManyFailures.saveAsTextFile(outputFolder1);

        /*
         * TASK B
         */

        JavaRDD<String> faultyServers = failuresIn2017
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                return new Tuple2<>(
                    new FailureAndMonth(tokens[2], Integer.parseInt(tokens[0].split("/")[1])),
                    new CountAndDowntime(1, Integer.parseInt(tokens[4]))
                );
            })                                                                      // Create pairs (server and month, 1)
            .reduceByKey((v1, v2) -> new CountAndDowntime(
                v1.getCount() + v2.getCount(),
                v1.getDowntime() + v2.getDowntime()
            ))                                                                      // Sum failures per server and month
            .filter(p -> p._2().getCount() >= 2)                                    // Filter out if not at least 2 failures
            .mapToPair(p -> new Tuple2<>(
                p._1().getServer(),
                new CountAndDowntime(1, p._2().getDowntime())
            ))                                                                      // Create pairs (server, 1) per each month
            .reduceByKey((v1, v2) -> new CountAndDowntime(
                v1.getCount() + v2.getCount(),
                v1.getDowntime() + v2.getDowntime()
            ))                                                                      // Sum months per server
            .filter(p -> p._2().getCount() == 12 && p._2().getDowntime() >= 1440)   // Filter out if not all months
            .map(p -> p._1());                                                      // Keep only the name of the server

            faultyServers.saveAsTextFile(outputFolder2);

        // Close the Spark context
        sc.close();
    }

}