/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import info.debatty.java.stringsimilarity.Cosine;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

/**
 *
 * @author Utilizador
 */
public class SimilarityAnalysis {

    /*
     Analysis Between 
     */
    public static void main(String[] args) {
        Connections conn = new Connections();
        Profiler prof = new Profiler("storesale_er", "income_band", conn);
        Profiler prof2 = new Profiler("storesale_er", "household_demographics", conn);
        List<String> out = new ArrayList<>();
        runSimilarityAnalysis(conn, prof.getDataSet(), prof2.getDataSet(), prof.getDataSet().columns(), out, 1, 0, prof.getDataSet().columns().length);
    }

    public static void runSimilarityAnalysis(Connections conn, Dataset<Row> datasetMain, Dataset<Row> datasetToCompare, String[] A, List<String> out, int k, int i, int n) {
        if (out.size() == k) {
            System.out.println("\n");
            System.out.println("ColumnMain: " + out.get(0));
            System.out.println("\n");
            for (String column : datasetToCompare.columns()) {

                Cosine cosineSim = new Cosine(2);
                org.apache.commons.text.similarity.JaccardSimilarity jaccardSim = new org.apache.commons.text.similarity.JaccardSimilarity();
                JaroWinkler jaroWinklerSimilarity = new JaroWinkler();
                NormalizedLevenshtein levenshteinSimilarity = new NormalizedLevenshtein();
                
                List<Row> columnA = datasetMain.select(col(out.get(0))).collectAsList();
                List<Row> columnB = datasetToCompare.select(col(column)).collectAsList();
                System.out.println("\t" + "---- SimilarityCosine" + "\t" + "ColumnToCompare: " + column + "---Value: " + cosineSim.similarity(columnA.toString(), columnB.toString()));
                System.out.println("\t" + "---- JaccardSimilarity" + "\t" + "ColumnToCompare: " + column + "---Value: " + jaccardSim.apply(columnA.toString(), columnB.toString()));
                System.out.println("\t" + "---- JaroWinkler" + "\t" + "ColumnToCompare: " + column + "---Value: " + jaroWinklerSimilarity.similarity(columnA.toString(), columnB.toString()));
                System.out.println("\t" + "---- NormalizedLevenshtein" + "\t" + "ColumnToCompare: " + column + "---Value: " + levenshteinSimilarity.similarity(columnA.toString(), columnB.toString()));
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
            runSimilarityAnalysis(conn, datasetMain, datasetToCompare, A, out, k, j, n);
            // backtrack - remove current element from solution
            out.remove(out.size() - 1);
//	    code to handle duplicates - skip adjacent duplicate elements
            while (j < n - 1 && A[j] == A[j + 1]) {
                j++;
            }
        }
    }

}
