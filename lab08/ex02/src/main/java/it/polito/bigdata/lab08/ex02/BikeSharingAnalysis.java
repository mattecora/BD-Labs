package it.polito.bigdata.lab08.ex02;

import java.sql.Timestamp;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class BikeSharingAnalysis {

    public static void main(String[] args) {
        // Parse command-line parameters
        String inputPath = args[0];
        String registerFile = inputPath + "/register.csv";
        String stationsFile = inputPath + "/stations.csv";
        String outputPath = args[1];
        Double threshold = Double.parseDouble(args[2]);

        // Create the Spark session
        SparkSession ss = SparkSession.builder()
            .appName("Lab 8.2 - Analysis of a bike sharing dataset using Spark SQL")
            .master("local")
            .getOrCreate();

        // Create the input Dataframe
        Dataset<Row> registerDataframe = ss.read()
            .format("csv")
            .option("delimiter", "\t")
            .option("inferSchema", true)
            .option("header", true)
            .load(registerFile);
    
        // Define UDFs
        ss.udf().register("critical", (Integer freeSlots) -> freeSlots == 0 ? 1 : 0, DataTypes.IntegerType);
        ss.udf().register("dayofweeknamed", (Timestamp date) -> DateTool.dayOfTheWeek(date.toString()), DataTypes.StringType);
        
        // Create temporary view
        try {
            registerDataframe.createTempView("register");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        // Run SQL code to compute criticality
        Dataset<Row> registerWithCriticality = ss.sql(
            "SELECT station, dayofweeknamed(timestamp) as dayoftheweek, hour(timestamp) as hours," +
            " sum(critical(free_slots)) / count(*) as criticality" +
            " FROM register" +
            " WHERE NOT (free_slots = 0 AND used_slots = 0)" +
            " GROUP BY station, dayoftheweek, hours" +
            " HAVING criticality > " + threshold
        );

        try {
            registerWithCriticality.createTempView("register_criticality");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        // Create the stations Dataframe
        Dataset<Row> stationsDataframe = ss.read()
            .format("csv")
            .option("delimiter", "\t")
            .option("inferSchema", true)
            .option("header", true)
            .load(stationsFile);
        
        // Create temporary view
        try {
            stationsDataframe.createTempView("stations");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        
        // Run SQL code to join and sort
        Dataset<Row> sortedJoinedDatasets = ss.sql(
            "SELECT station, dayoftheweek, hours, criticality, longitude, latitude" +
            " FROM register_criticality, stations" +
            " WHERE register_criticality.station = stations.id" +
            " ORDER BY criticality DESC, station ASC, dayoftheweek ASC, hours ASC");
        
        // Store the result
        sortedJoinedDatasets.write()
            .format("csv")
            .option("delimiter", "\t")
            .option("header", true)
            .save(outputPath);
        
        ss.stop();
        ss.close();
    }
}
