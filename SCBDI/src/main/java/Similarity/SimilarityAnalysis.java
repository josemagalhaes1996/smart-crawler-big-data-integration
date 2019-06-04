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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

/**
 *
 * @author Utilizador
 *
 */
public class SimilarityAnalysis {

    /**
     *
     * Analysis between two different tables Intra and Inter Analysis attributes
     * Mesures: Jaccard,Cosine, Levenshtein, Jaro-Winklet
     */
    public static void main(String[] args) {
        Connections conn = new Connections();
        Profiler prof = new Profiler("storesale_er", "store_sales", conn);
//        Profiler prof2 = new Profiler("storesale_st", "household_demographics", conn);

        List<String> out = new ArrayList<>();
        String path = "/user/jose/storesale_er/promotion/promotion.csv";
        String delimiter = ";";
        String header = "true";
        Dataset<Row> newDataSource = conn.getSession().read().format("csv").option("header", header).option("delimiter", delimiter).option("inferSchema", "true").load(path);

//        runSimilarityIntraColumn(conn, prof2.getDataSet(), prof2.getDataSet().columns(), out, 2, 0, prof2.getDataSet().columns().length);
        runSimilarityInterAnalysis(conn, newDataSource, prof.getDataSet());

    }

    public static void runSimilarityInterAnalysis(Connections conn, Dataset<Row> newDataSource, Dataset<Row> BDWSource) {

        Cosine cosineSim = new Cosine(2);
        org.apache.commons.text.similarity.JaccardSimilarity jaccardSim = new org.apache.commons.text.similarity.JaccardSimilarity();
        JaroWinkler jaroWinklerSimilarity = new JaroWinkler();
        NormalizedLevenshtein levenshteinSimilarity = new NormalizedLevenshtein();

        for (String columnNewSource : newDataSource.columns()) {
            for (String columnBDW : BDWSource.columns()) {
                System.out.println("\n" + " ColumnMain: " + columnNewSource);

               List<Row> newSourceRows = newDataSource.select(columnNewSource).collectAsList();
//                RDD<Row> BDWRows = BDWSource.select(columnBDW).;
                   System.out.println(Arrays.asList(BDWSource.select(columnBDW).collectAsList()));
//                System.out.println(newSourceRows.toString());     
//                System.out.println("\t" + "---- SimilarityCosine" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + cosineSim.similarity("1", "1"));
//                System.out.println("\t" + "---- JaccardSimilarity" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + jaccardSim.apply(newSourceRows.toString(), BDWRows.toString()));
//                System.out.println("\t" + "---- JaroWinkler" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + jaroWinklerSimilarity.similarity(newSourceRows.toString(), BDWRows.toString()));
//                System.out.println("\t" + "---- NormalizedLevenshtein" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + levenshteinSimilarity.similarity(newSourceRows.toString(), BDWRows.toString()));
//                System.out.println("\n");
            }
        }
    }

    
    public static void runSimilarityIntraColumn(Connections conn, Dataset<Row> dataset, String[] A, List<String> out, int k, int i, int n) {
        if (out.size() == k) {
            if (out.get(0) == out.get(1)) {
            } else {
                Cosine cosineSim = new Cosine(2);
                org.apache.commons.text.similarity.JaccardSimilarity jaccardSim = new org.apache.commons.text.similarity.JaccardSimilarity();
                JaroWinkler jaroWinklerSimilarity = new JaroWinkler();
                NormalizedLevenshtein levenshteinSimilarity = new NormalizedLevenshtein();

                List<Row> columnA = dataset.select(col(out.get(0))).collectAsList();
                List<Row> columnB = dataset.select(col(out.get(1))).collectAsList();
                //      sim.apply(columnA.toString(), columnB.toString());
                System.out.println("\t" + "---- SimilarityCosine" + "\t" + "----ColumnMain: " + out.get(0) + " ColumnToCompare: " + out.get(1) + "---Value: " + cosineSim.similarity(columnA.toString(), columnB.toString()));
                System.out.println("\t" + "---- JaccardSimilarity" + "\t" + "----ColumnMain: " + out.get(0) + " ColumnToCompare: " + out.get(1) + "---Value: " + jaccardSim.apply(columnA.toString(), columnB.toString()));
                System.out.println("\t" + "---- JaroWinkler" + "\t" + "----ColumnMain: " + out.get(0) + " ColumnToCompare: " + out.get(1) + "---Value: " + jaroWinklerSimilarity.similarity(columnA.toString(), columnB.toString()));
                System.out.println("\t" + "---- NormalizedLevenshtein" + "----ColumnMain: " + out.get(0) + " ColumnToCompare: " + out.get(1) + "---Value: " + levenshteinSimilarity.similarity(columnA.toString(), columnB.toString()));
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
            runSimilarityIntraColumn(conn, dataset, A, out, k, j, n);
            // backtrack - remove current element from solution
            out.remove(out.size() - 1);
//	    code to handle duplicates - skip adjacent duplicate elements
            while (j < n - 1 && A[j] == A[j + 1]) {
                j++;
            }
        }
    }

}
