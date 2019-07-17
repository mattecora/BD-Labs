package it.polito.bigdata.exams.ex20170914.spark;

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
        SparkConf conf = new SparkConf().setAppName("Exam 20170914 - Spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the input files
        JavaRDD<String> flights = sc.textFile(inputFile1).cache();
        JavaRDD<String> airports = sc.textFile(inputFile2).filter(s -> s.startsWith("2017")).cache();

        /*
         * TASK A
         */

        // List the German airports
        JavaPairRDD<String, String> germanAirports = airports
            .filter(s -> s.endsWith("Germany"))
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                return new Tuple2<>(tokens[0], tokens[1]);
            });
        
        // Select delayed flights landing in Germany
        JavaRDD<String> delayedFlightsLandingInGermany = flights
            .filter(s -> {
                String[] tokens = s.split(",");
                return Integer.parseInt(tokens[7]) >= 15;
            })                                                                  // Filter out non-late flights
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                return new Tuple2<>(tokens[7], tokens[1]);
            })                                                                  // Create pairs (destcode, airline)
            .join(germanAirports)                                               // Join German airports with name
            .mapToPair(p -> new Tuple2<>(
                new AirlineAndDestination(p._2()._1(), p._2()._2()),
                1
            ))                                                                  // Create pairs ((airline, destname), 1)
            .reduceByKey((v1, v2) -> v1 + v2)                                   // Reduce to ((airline, destname), total)
            .mapToPair(p -> new Tuple2<>(p._2(), p._1()))                       // Create pairs (total, (airline, destname))
            .sortByKey()                                                        // Sort by total delayed flights
            .map(p -> p._1() + "," + p._2().getAirline() + "," + p._2().getDestination());  // Map to required format
        
        delayedFlightsLandingInGermany.saveAsTextFile(outputFolder1);
        
        /*
         * TASK B
         */
        
        JavaRDD<String> overloadedRoutes = flights
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                Integer full = Integer.parseInt(tokens[9]) == Integer.parseInt(tokens[10]) ? 1 : 0;
                Integer cancelled = tokens[8].equals("yes") ? 1 : 0;

                return new Tuple2<>(new Route(tokens[5], tokens[6]), new FullAndCancelled(full, cancelled, 1));
            })                                                                  // Create pairs ((dep, arr), (full, canc, 1))
            .reduceByKey((v1, v2) -> new FullAndCancelled(
                v1.getFull() + v2.getFull(),
                v1.getCancelled() + v2.getCancelled(),
                v1.getTotal() + v2.getTotal()
            ))                                                                  // Reduce to ((dep, arr), (tot full, tot canc, tot))
            .filter(p -> 
                (double) p._2().getFull() / p._2().getTotal() >= 0.99 &&
                (double) p._2().getCancelled() / p._2().getTotal() >= 0.05
            )                                                                   // Filter out if full% < 99% or canc% < 5%
            .map(p -> p._1().getDeparture() + "," + p._1().getArrival());       // Map to required format

        overloadedRoutes.saveAsTextFile(outputFolder2);

        // Close the Spark context
        sc.close();
    }

}