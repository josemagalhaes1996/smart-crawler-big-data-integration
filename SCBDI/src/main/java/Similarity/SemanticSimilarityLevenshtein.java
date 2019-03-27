/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import basicProfiler.ColumnProfiler;
import com.hortonworks.hwc.Connections;
import basicProfiler.Profiler;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.levenshtein;

/**
 *
 * @author Utilizador
 */
public class SemanticSimilarityLevenshtein implements Serializable {

    private String attributeA;
    private String levenshteinDistance;
    private String attributeB;

    public SemanticSimilarityLevenshtein(String attribute, String similarityValue, String attributebdw) {
        this.attributeA = attribute;
        this.levenshteinDistance = similarityValue;
        this.attributeB = attributebdw;
    }

    public SemanticSimilarityLevenshtein() {
    }

    public String getAttributeA() {
        return attributeA;
    }

    public void setAttributeA(String attributeA) {
        this.attributeA = attributeA;
    }

    public String getLevenshteinDistance() {
        return levenshteinDistance;
    }

    public void setLevenshteinDistance(String levenshteinDistance) {
        this.levenshteinDistance = levenshteinDistance;
    }

    public String getAttributeB() {
        return attributeB;
    }

    public void setAttributeB(String attributeB) {
        this.attributeB = attributeB;
    }

    public static void main(String args[]) {
        Connections conn = new Connections();
        Profiler prof = new Profiler("tpcds", "item", conn);
        List<String> out = new ArrayList<>();
        runLevenshteinSimilarity(conn, prof.getDataSet(), prof.getDataSet().columns(), out, 2, 0, prof.getDataSet().columns().length);
    }

    public static void runLevenshteinSimilarity(Connections conn, Dataset<Row> dataset, String[] A, List<String> out, int k, int i, int n) {

        if (out.size() == k) {
            if (out.get(0) == out.get(1)) {
            } else {
                Dataset<Row> df2 = dataset.withColumn("LevenshteinSimilarity", levenshtein(col(out.get(0)), col(out.get(1))));
                List<Row> listLevenshteinDistance = df2.select(col("LevenshteinSimilarity")).collectAsList();
                double similarity = 0;
                int countValidNumbers = 0;
                for (Row r : listLevenshteinDistance) {
                    if (r.mkString().equals("null")) {

                    } else {
                        similarity = similarity + Double.parseDouble(r.mkString());
                        countValidNumbers = countValidNumbers + 1;
                    }
                }
                double LevenshteinSimilarity = (double) similarity / countValidNumbers;
                //  df2.show();
                System.out.println("----ColumnMain: " + out.get(0) + "ColumnToCompare: " + out.get(1) + "---Value: " + LevenshteinSimilarity);
                System.out.println("\n");
            }
            return;
        }
        // start from previous element in the current combination
        // till last element
        for (int j = i; j < n; j++) {
            // add current element A[j] to the solution and recurse with
            // same index j (as repeated elements are allowed in combinations)
            out.add(A[j]);
            runLevenshteinSimilarity(conn, dataset, A, out, k, j, n);
            // backtrack - remove current element from solution
            out.remove(out.size() - 1);
//	    code to handle duplicates - skip adjacent duplicate elements
            while (j < n - 1 && A[j] == A[j + 1]) {
                j++;
            }
        }
    }

}
