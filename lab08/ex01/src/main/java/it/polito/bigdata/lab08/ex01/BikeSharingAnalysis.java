package it.polito.bigdata.lab08.ex01;

import java.sql.Timestamp;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

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
            .appName("Lab 8.1 - Analysis of a bike sharing dataset using Spark SQL")
            .getOrCreate();

        // Create the input Dataframe
        Dataset<Row> registerDataframe = ss.read()
            .format("csv")
            .option("delimiter", "\t")
            .option("inferSchema", true)
            .option("header", true)
            .load(registerFile);
        
        // Make it a Dataset
        Dataset<Entry> registerDataset = registerDataframe.map((MapFunction<Row, Entry>) r -> {
            Timestamp timestamp = r.getTimestamp(1);

            // Extract information from timestamp
            String dayOfTheWeek = DateTool.dayOfTheWeek(timestamp);
            Integer hours = DateTool.hour(timestamp);

            // Produce the Entry object
            Entry e = new Entry();
            e.setStation(r.getInt(0));
            e.setDayOfTheWeek(dayOfTheWeek);
            e.setHours(hours);
            e.setUsedSlots(r.getInt(2));
            e.setFreeSlots(r.getInt(3));
            e.setCritical(e.getFreeSlots() == 0 ? 1 : 0);

            return e;
        }, Encoders.bean(Entry.class))
        .filter((FilterFunction<Entry>) e -> !(e.getUsedSlots() == 0 && e.getFreeSlots() == 0));

        // Group by station and timeslot
        RelationalGroupedDataset groupedRegister = registerDataset.groupBy("station", "dayOfTheWeek", "hours");

        // Compute criticality
        Dataset<CriticalityEntry> registerWithCriticality = groupedRegister.agg(
            sum("critical").divide(count("*")).alias("criticality"))
        .filter((FilterFunction<Row>) r -> r.getDouble(3) > threshold)
        .as(Encoders.bean(CriticalityEntry.class));
        // Create the stations Dataframe
        Dataset<Row> stationsDataframe = ss.read()
            .format("csv")
            .option("delimiter", "\t")
            .option("inferSchema", true)
            .option("header", true)
            .load(stationsFile);
        
        // Make it a Dataset
        Dataset<Station> stationsDataset = stationsDataframe.as(Encoders.bean(Station.class));

        // Join the two datasets
        Dataset<Row> joinedDatasets = registerWithCriticality.join(stationsDataset, 
            registerWithCriticality.col("station").equalTo(stationsDataset.col("id")));
        
        // Sort the resulting dataset and select only interesting columns
        Dataset<Row> sortedJoinedDatasets = joinedDatasets
            .sort(new Column("criticality").desc(), new Column("station").asc(), new Column("dayOfTheWeek").asc(), new Column("hours").asc())
            .select("station", "dayOfTheWeek", "hours", "criticality", "longitude", "latitude");
        
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
