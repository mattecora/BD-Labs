package it.polito.bigdata.lab09.ex04;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;

public class FineFoodAnalysis {

    private final static Double THRESHOLD = 0.9;

    public static void main(String[] args) {
        // Parse command-line parameters
        String inputPath = args[0];

        // Create the Spark session
        SparkSession ss = SparkSession.builder()
            .appName("Lab 9.2 - Classification on the Amazon fine-foods dataset (single feature)")
            .getOrCreate();
        
        // Create the Spark context
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

        // Create the input Dataframe
        Dataset<Row> reviewsDataframe = ss.read()
            .format("csv")
            .option("delimiter", ",")
            .option("inferSchema", true)
            .option("header", true)
            .load(inputPath);

        // Create the labeled points RDD
        JavaRDD<LabeledDocument> labeledDocumentsRDD = reviewsDataframe
            .toJavaRDD()
            .filter(r -> r.getInt(5) != 0)
            .map(r -> {
                // Compute the helpfulness index
                Double helpfulness = (double) r.getInt(4) / r.getInt(5);

                // Emit the labeled point
                return new LabeledDocument(helpfulness < THRESHOLD ? 0.0 : 1.0, r.getString(9));
            });
        
        // Create the labeled points Dataframe
        Dataset<Row> labeledDocumentsDataframe = ss.createDataFrame(labeledDocumentsRDD, LabeledDocument.class);
        
        // Display 5 example rows
        labeledDocumentsDataframe.show(5);
        
        // Split the data into training and test sets (30% held out for testing)
        Dataset<Row>[] splits = labeledDocumentsDataframe.randomSplit(new double[]{ 0.7, 0.3 });
        Dataset<Row> trainingData = splits[0].cache();
        Dataset<Row> testData = splits[1].cache();

        // Define the tokenizer
        Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");

        // Define the stop words remover
        StopWordsRemover stopWordsRemover = new StopWordsRemover().setInputCol("words").setOutputCol("relevantWords");

        // Define the hashing TF transformation
        HashingTF hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("relevantWords").setOutputCol("rawFeatures");

        // Define the IDF transformation
        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");

        // Define the classification algorithm
        LogisticRegression lr = new LogisticRegression();
        lr.setMaxIter(10);
        lr.setRegParam(0.01);

        // Create the pipeline
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{ tokenizer, stopWordsRemover, hashingTF, idf, lr });
        
        // Train the model
        PipelineModel model = pipeline.fit(trainingData);
        
        // Apply the model to the test set
        Dataset<Row> predictions = model.transform(testData);

        // Compute the quality metrics
        MulticlassMetrics metrics = new MulticlassMetrics(predictions.select("prediction", "label"));

        // Show the confusion matrix
        Matrix confusion = metrics.confusionMatrix();
        System.out.println("Confusion matrix: \n" + confusion);

        // Show the overall accuracy
        double accuracy = metrics.accuracy();
        System.out.println("Accuracy = " + accuracy);
            
        // Close the Spark context
        sc.close();
        
        // Stop the Spark session
        ss.stop();
        ss.close();
    }
}
