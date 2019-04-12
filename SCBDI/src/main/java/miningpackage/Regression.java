/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miningpackage;

import com.hortonworks.hwc.Connections;
import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;

/**
 *
 * @author Utilizador
 */
public class Regression {

    public static void main(String args[]) {

        Connections conn = new Connections();

// Load and parse the data file.
        String datapath = "data/mllib/sample_libsvm_data.txt";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(conn.getJavasparkContext().sc(), datapath).toJavaRDD();
// Split the data into training and test sets (30% held out for testing)
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

// Set parameters.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
        int numClasses = 2;
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        String impurity = "gini";
        int maxDepth = 5;
        int maxBins = 32;

// Train a DecisionTree model for classification.
        DecisionTreeModel model = DecisionTree.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, impurity, maxDepth, maxBins);

// Evaluate model on test instances and compute test error
        JavaPairRDD<Double, Double> predictionAndLabel
                = testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
        double testErr
                = predictionAndLabel.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testData.count();

        System.out.println("Test Error: " + testErr);
        System.out.println("Learned classification tree model:\n" + model.toDebugString());

// Save and load model
        model.save(conn.getJavasparkContext().sc(), "target/tmp/myDecisionTreeClassificationModel");
        DecisionTreeModel sameModel = DecisionTreeModel
                .load(conn.getJavasparkContext().sc(), "target/tmp/myDecisionTreeClassificationModel");

    }
}
