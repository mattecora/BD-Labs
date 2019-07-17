package it.polito.bigdata.exams.ex20190215.spark;

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

        // Create the Spark context
        SparkConf conf = new SparkConf().setAppName("Exam 20190215 - Spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the input file
        JavaRDD<String> poisRDD = sc.textFile(inputFile).cache();

        /*
         * TASK A
         */

        JavaRDD<String> citiesWithBuses = poisRDD
            .flatMap(s -> {
                List<String> returnedValues = new ArrayList<>();

                // Split the input string
                String[] tokens = s.split(",");

                // Check if the city is Italian and the subcategory is Busstop
                if (tokens[4].equals("Italy") && tokens[6].equals("Busstop"))
                    returnedValues.add(tokens[3]);

                return returnedValues.iterator();
            })
            .distinct();

        JavaRDD<String> citiesWithTaxiAndWithoutBuses = poisRDD
            .flatMap(s -> {
                List<String> returnedValues = new ArrayList<>();

                // Split the input string
                String[] tokens = s.split(",");

                // Check if the city is Italian and the subcategory is Taxi
                if (tokens[4].equals("Italy") && tokens[6].equals("Taxi"))
                    returnedValues.add(tokens[3]);

                return returnedValues.iterator();
            })
            .distinct()
            .subtract(citiesWithBuses);

        citiesWithTaxiAndWithoutBuses.saveAsTextFile(outputFolder1);

        /*
         * TASK B
         */

        // Count cities
        final Long numberOfCities = poisRDD
            .filter(s -> s.split(",")[4].equals("Italy"))
            .map(s -> s.split(",")[3])
            .distinct()
            .count();

        // Count museums
        final Long numberOfMuseums = poisRDD
            .filter(s -> {
                String[] tokens = s.split(",");
                return tokens[4].equals("Italy") && tokens[6].equals("Museum");
            })
            .map(s -> s.split(",")[3])
            .distinct()
            .count();

        // Compute average museums per city
        final Double avgMuseumsPerCity = (double) numberOfMuseums / numberOfCities;

        // Select cities with more museums
        JavaRDD<String> citiesWithMoreMuseums = poisRDD
            .filter(s -> {
                String[] tokens = s.split(",");
                return tokens[4].equals("Italy") && tokens[6].equals("Museum");
            })
            .mapToPair(s -> new Tuple2<>(s.split(",")[3], 1))
            .reduceByKey((v1, v2) -> v1 + v2)
            .filter(p -> p._2() > avgMuseumsPerCity)
            .map(p -> p._1())
            .distinct();
        
        citiesWithMoreMuseums.saveAsTextFile(outputFolder2);

        // Close the Spark context
        sc.close();
    }

}