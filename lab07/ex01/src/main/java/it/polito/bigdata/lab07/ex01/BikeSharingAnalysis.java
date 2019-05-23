package it.polito.bigdata.lab07.ex01;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import scala.Tuple2;

public class BikeSharingAnalysis {

    public static void main(String[] args) {
        // Parse command-line parameters
        String inputPath = args[0];
        String registerFile = inputPath + "/register.csv";
        String stationsFile = inputPath + "/stations.csv";
        String outputPath = args[1];
        Double threshold = Double.parseDouble(args[2]);

        // Create the Spark config
        SparkConf conf = new SparkConf().setAppName("Lab 7.1 - Analysis of a bike sharing dataset");

        // Create the Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create the input RDD
        JavaRDD<String> register = sc.textFile(registerFile);

        // Construct the ((stationId, timestamp), freeSlots) pairs
        JavaPairRDD<Tuple2<String, String>, Integer> registerPairs = register
            .filter(s -> !s.startsWith("station"))              // remove first line
            .filter(s -> !s.endsWith("0\t0"))                   // remove wrong readings
            .flatMapToPair(s -> {
                List<Tuple2<Tuple2<String, String>, Integer>> pairs = new ArrayList<>();

                try {
                    String[] tokens = s.split("\t");
                    String dayOfTheWeek = DateTool.DayOfTheWeek(tokens[1]);
                    pairs.add(new Tuple2<>(
                        new Tuple2<>(tokens[0], dayOfTheWeek + " - " + tokens[1].split(" ")[1].split(":")[0]),
                        Integer.parseInt(tokens[3])));
                } catch (Exception e) {
                    System.err.println("Skipping incorrect record: " + s);
                }

                return pairs.iterator();
            });
        
        // Collect statistics by key and filter criticalities
        JavaPairRDD<Tuple2<String, String>, Double> criticalityStationTimeslot = registerPairs
            .aggregateByKey(
                new Average(),
                (a, i) -> {
                    Average ret = new Average(a);
                    if (i == 0) ret.addSum(1);
                    ret.addTotal(1);
                    return ret;
                },
                (a1, a2) -> {
                    Average ret = new Average(a1);
                    ret.addSum(a2.getSum());
                    ret.addTotal(a2.getTotal());
                    return ret;
                }
            )
            .mapValues(a -> a.getAverage())
            .filter(p -> p._2() > threshold);
        
        // Retrieve the most critical timeslots per each station
        JavaPairRDD<String, Tuple2<String, Double>> mostCriticalTimeslots = criticalityStationTimeslot
            .mapToPair(p -> new Tuple2<>(p._1()._1(), new Tuple2<>(p._1()._2(), p._2())))
            .reduceByKey((v1, v2) -> {
                // Consider only the criticality value
                Double avg1 = v1._2(), avg2 = v2._2();
                if (avg1 < avg2) return v2;
                if (avg1 > avg2) return v1;
                
                // Consider the earlier timeslot
                Integer ts1 = Integer.parseInt(v1._1().split("-")[1]), ts2 = Integer.parseInt(v2._1().split("-")[1]);
                if (ts1 < ts2) return v1;
                if (ts1 > ts2) return v1;

                // Consider the day of the week
                String day1 = v1._1().split("-")[0], day2 = v1._1().split("-")[0];
                if (day1.compareTo(day2) < 0) return v1;
                return v2;
            });

        // Read the stations information and join with the critical timeslots
        JavaPairRDD<String, Tuple2<Coordinate, Tuple2<String, Double>>> criticalStationsInfo = sc.textFile(stationsFile)
            .filter(s -> !s.startsWith("id"))
            .flatMapToPair(s -> {
                List<Tuple2<String, Coordinate>> pairs = new ArrayList<>();
                
                try {
                    String[] tokens = s.split("\t");
                    pairs.add(new Tuple2<>(
                        tokens[0], 
                        new Coordinate(Double.parseDouble(tokens[1]), Double.parseDouble(tokens[2])))
                    );
                } catch (Exception e) {
                    System.err.println("Skipping incorrect record: " + s);
                }

                return pairs.iterator();
            })
            .join(mostCriticalTimeslots);
        
        // Produce the KML strings
        List<String> lines = criticalStationsInfo
            .map(p -> {
                return "<Placemark><name>" + p._1() +"</name><ExtendedData>" +
                "<Data name=\"DayWeek\"><value>" + p._2()._2()._1().split("-")[0] + "</value></Data>" +
                "<Data name=\"Hour\"><value>" + p._2()._2()._1().split("-")[1] + "</value></Data>" +
                "<Data name=\"Criticality\"><value>" + p._2()._2()._2() + "</value></Data></ExtendedData>"
                + "<Point><coordinates>" + p._2()._1().getLatitude() + "," + p._2()._1().getLongitude() + "</coordinates></Point></Placemark>";
            })
            .collect();
        
        writeKML(outputPath + "/output.kml", lines);

        // Close the Spark context
        sc.close();
    }

    public static void writeKML(String outputFilename, List<String> lines) {
        Configuration confHadoop = new Configuration();

		try {
            URI uri = URI.create(outputFilename);

            FileSystem file = FileSystem.get(uri, confHadoop);
            FSDataOutputStream outputFile = file.create(new Path(uri));

            BufferedWriter bOutFile = new BufferedWriter(new OutputStreamWriter(outputFile, "UTF-8"));

            // Header
            bOutFile.write("<kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document>");
            bOutFile.newLine();

            // Markers
            for (String lineKML : lines) {
                bOutFile.write(lineKML);
                bOutFile.newLine();
            }

            // Footer
            bOutFile.write("</Document></kml>");
            bOutFile.newLine();

            bOutFile.close();
            outputFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
