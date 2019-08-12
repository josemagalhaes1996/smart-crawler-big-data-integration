/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import info.debatty.java.stringsimilarity.Cosine;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;


/**
 *
 * @author Utilizador
 */
public class CosineSimilarity {

    private String attributeA;
    private String cosineSimilarity;
    private String attributeB;

    public CosineSimilarity(String attribute, String similarityValue, String attributebdw) {
        this.attributeA = attribute;
        this.cosineSimilarity = similarityValue;
        this.attributeB = attributebdw;
    }

    public CosineSimilarity() {
    }

    public String getAttributeA() {
        return attributeA;
    }

    public void setAttributeA(String attributeA) {
        this.attributeA = attributeA;
    }

    public String getCosineSimilarity() {
        return cosineSimilarity;
    }

    public void setCosineSimilarity(String cosineSimilarity) {
        this.cosineSimilarity = cosineSimilarity;
    }

    public String getAttributeB() {
        return attributeB;
    }

    public void setAttributeB(String attributeB) {
        this.attributeB = attributeB;
    }

    public static void main(String args[]) {
        Instant start = Instant.now();         
        Connections conn = new Connections();
        Profiler prof = new Profiler("storesale_er", "store_sales", conn);
        Profiler prof2 = new Profiler("storesale_er", "promotion", conn);
        
        List<String> out = new ArrayList<>();
//      runCosineSimilarity(conn, prof.getDataSet(), prof.getDataSet().columns(), out, 2, 0, prof.getDataSet().columns().length);
//      runCosineSimilarityInterColumn(conn, prof.getDataSet(), prof2.getDataSet(), prof.getDataSet().columns(), out, 1, 0, prof.getDataSet().columns().length);
        runSimilaritiesInterColumn(conn, prof.getDataSet(), prof2.getDataSet());
        Instant end = Instant.now().minus(start.getEpochSecond(), ChronoUnit.SECONDS);
        System.out.println(end.getEpochSecond());
    }

    /**
     * Implements Cosine Similarity between strings. The strings are first
     * transformed in vectors of occurrences of k-shingles (sequences of k
     * characters). In this n-dimensional space, the similarity between the two
     * strings is the cosine of their respective vectors.
     *
     */
    public static void runCosineSimilarity(Connections conn, Dataset<Row> dataset, String[] A, List<String> out, int k, int i, int n) {
        if (out.size() == k) {
            if (out.get(0) == out.get(1)) {
            } else {
                Cosine sim = new Cosine(2);
                JavaRDD<Row> columnA = dataset.select(col(out.get(0))).toJavaRDD();
                JavaRDD<Row> columnB = dataset.select(col(out.get(1))).toJavaRDD();
                //      sim.apply(columnA.toString(), columnB.toString());
                System.out.println("----ColumnMain: " + out.get(0) + "ColumnToCompare: " + out.get(1) + "---Value: " + sim.similarity(columnA.toString(), columnB.toString()));
            }
            return;
        }
        // start from previous element in the current combination
        // till last element
        for (int j = i; j < n; j++) {
            // add current element A[j] to the solution and recurse with
            // same index j (as repeated elements are allowed in combinations)
            out.add(A[j]);
            runCosineSimilarity(conn, dataset, A, out, k, j, n);
            // backtrack - remove current element from solution
            out.remove(out.size() - 1);
//	    code to handle duplicates - skip adjacent duplicate elements
            while (j < n - 1 && A[j] == A[j + 1]) {
                j++;
            }
        }
    }

    public static void runSimilaritiesInterColumn(Connections conn, Dataset<Row> datasetMain, Dataset<Row> datasetToCompare) {
        for (int i = 0; i < datasetMain.columns().length; i++) {
            System.out.println("ColumnMain: " + datasetMain.columns()[i]);
            System.out.println("\n");
            List<Row> columnA = datasetMain.select(col(datasetMain.columns()[i])).collectAsList();
            System.out.println(columnA.toString());
            for (int j = 0; j < datasetToCompare.columns().length; j++) {
                Cosine sim = new Cosine(2);
                List<Row> columnB = datasetToCompare.select(col(datasetToCompare.columns()[j])).collectAsList();
                System.out.println("\t" + "---- " + "ColumnToCompare: " + datasetToCompare.columns()[j] + "---Value: " + sim.similarity(columnA.toString(), columnB.toString()));

            }
        }

    }

    public static void runCosineSimilarityInterColumn(Connections conn, Dataset<Row> datasetMain, Dataset<Row> datasetToCompare, String[] A, List<String> out, int k, int i, int n) {
        if (out.size() == k) {
            System.out.println("ColumnMain: " + out.get(0));
            System.out.println("\n");
            for (String column : datasetToCompare.columns()) {

                Cosine sim = new Cosine(2);
                JavaRDD<Row> columnA = datasetMain.select(col(out.get(0))).toJavaRDD();
                JavaRDD<Row> columnB = datasetToCompare.select(col(column)).toJavaRDD();
                System.out.println("\t" + "---- " + "ColumnToCompare: " + column + "---Value: " + sim.similarity(columnA.toString(), columnB.toString()));
            }

            return;
        }
        // start from previous element in the current combination
        // till last element
        for (int j = i; j < n; j++) {
            // add current element A[j] to the solution and recurse with
            // same index j (as repeated elements are allowed in combinations)
            out.add(A[j]);
            runCosineSimilarityInterColumn(conn, datasetMain, datasetToCompare, A, out, k, j, n);
            // backtrack - remove current element from solution
            out.remove(out.size() - 1);
//	    code to handle duplicates - skip adjacent duplicate elements
            while (j < n - 1 && A[j] == A[j + 1]) {
                j++;
            }
        }
    }

}
