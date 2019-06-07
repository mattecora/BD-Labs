package it.polito.bigdata.lab09.ex01;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.feature.LabeledPoint;

public class FineFoodAnalysis {

    private static final Double THRESHOLD = 0.9;

    public static void main(String[] args) {
        // Parse command-line parameters
        String inputPath = args[0];
        
        // Create the Spark session
        SparkSession ss = SparkSession.builder()
                .appName("Lab 9.1 - Classification on the Amazon fine-foods dataset (preprocessing)")
                .getOrCreate();

        // Create the Spark context
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

        // Create the input Dataframe
        Dataset<Row> reviewsDataframe = ss.read().format("csv").option("delimiter", ",").option("inferSchema", true)
                .option("header", true).load(inputPath);

        // Create the labeled points RDD
        JavaRDD<LabeledPoint> labeledPointsRDD = reviewsDataframe.toJavaRDD().filter(r -> r.getInt(5) != 0).map(r -> {
            // Compute the review length
            Integer length = r.getString(9).length();

            // Compute the helpfulness index
            Double helpfulness = (double) r.getInt(4) / r.getInt(5);

            // Emit the labeled point
            return new LabeledPoint(helpfulness < THRESHOLD ? 0.0 : 1.0, Vectors.dense(length));
        });

        // Create the labeled points Dataframe
        Dataset<Row> labeledPointsDataframe = ss.createDataFrame(labeledPointsRDD, LabeledPoint.class);
        
        // Display 5 example rows
        labeledPointsDataframe.show(5);
            
        // Close the Spark context
        sc.close();
        
        // Stop the Spark session
        ss.stop();
        ss.close();
    }
}
