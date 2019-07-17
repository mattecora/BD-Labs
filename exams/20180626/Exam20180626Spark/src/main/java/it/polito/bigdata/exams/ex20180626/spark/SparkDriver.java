package it.polito.bigdata.exams.ex20180626.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

    public static void main(String[] args) {
        Double cpuThr = Double.parseDouble(args[0]), ramThr = Double.parseDouble(args[1]);
        String inputFile = args[2];
        String outputFolder1 = args[3], outputFolder2 = args[4];

        // Create the Spark context
        SparkConf conf = new SparkConf().setAppName("Exam 20180626 - Spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the input file
        JavaRDD<String> serverDataFromMay2018 = sc
            .textFile(inputFile)
            .filter(s -> s.startsWith("2018/05"))
            .cache();

        /*
         * TASK A
         */

        JavaRDD<String> criticalPairs = serverDataFromMay2018
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                return new Tuple2<>(
                    tokens[2] + "_" + tokens[1].split(":")[0],
                    new CpuAndRam(1, Double.parseDouble(tokens[3]), Double.parseDouble(tokens[4]))
                );
            })                                                                  // Create pairs (VSID_hour, (1, CPU, RAM))
            .reduceByKey((v1, v2) -> new CpuAndRam(
                v1.getCount() + v2.getCount(),
                v1.getCpu() + v2.getCpu(),
                v1.getRam() + v2.getRam()
            ))                                                                  // Reduce to pairs (VSID_hour, (count, tot CPU, tot RAM))
            .filter(p -> p._2().getCpu() / p._2().getCount() >= cpuThr &&
                p._2().getRam() / p._2().getCount() >= ramThr)                  // Filter pairs having avg(CPU) > thr and avg(RAM) > thr
            .keys();                                                            // Maintain the keys only

        criticalPairs.saveAsTextFile(outputFolder1);

        /*
         * TASK B
         */

        JavaRDD<String> unbalancedServers = serverDataFromMay2018
            .flatMapToPair(s -> {
                List<Tuple2<ServerDateAndHour, Double>> returnedValues = new ArrayList<>();

                String[] tokens = s.split(",");
                Double cpuUsage = Double.parseDouble(tokens[3]);

                // Emit a pair only if it is interesting
                if (cpuUsage > 90 || cpuUsage < 10) {
                    returnedValues.add(new Tuple2<>(
                        new ServerDateAndHour(tokens[2], tokens[0], Integer.parseInt(tokens[1].split(":")[0])),
                        cpuUsage
                    ));
                }

                return returnedValues.iterator();
            })                                                                  // Create pairs ((VSID, date, hour), CPU)
            .reduceByKey((v1, v2) -> v1 > v2 ? v1 : v2)                         // Reduce to ((VSID, date, hour), max CPU)
            .mapToPair(p -> new Tuple2<>(
                p._1().getServer() + "_" + p._1().getDate(),
                p._2() > 90 ? new AboveAndUnder(1, 0) : new AboveAndUnder(0, 1)
            ))                                                                  // Create pairs (VSID_date, (1, 0)) or (VSID_date, (0, 1))
            .reduceByKey((v1, v2) -> new AboveAndUnder(
                v1.getAbove() + v2.getAbove(),
                v1.getUnder() + v2.getUnder()
            ))                                                                  // Reduce to (VSID_date, (tot above, tot under))
            .filter(p -> p._2().getAbove() >= 8 && p._2().getUnder() >= 8)      // Filter if above < 8 and under < 8
            .keys();                                                            // Maintain the keys only

        unbalancedServers.saveAsTextFile(outputFolder2);

        // Close the Spark context
        sc.close();
    }

}