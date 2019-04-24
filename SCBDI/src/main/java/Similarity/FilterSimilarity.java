/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import basicProfiler.ColumnProfiler;
import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import info.debatty.java.stringsimilarity.Cosine;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author Utilizador
 */
public class FilterSimilarity {

    public static void main(String args[]) {

        Connections conn = new Connections();
        Profiler prof = new Profiler("storesale_er", "store_sales", conn);
        String path = "/user/jose/storesale_er/promotion/promotion.csv";
        String delimiter = ";";
        Dataset<Row> dataset = conn.getSession().read().format("csv").option("header", "true").option("delimiter", delimiter).option("inferSchema", "true").load(path);
        filterPairs(dataset, prof.getDataSet());
        

    }

    //Fuzzy Score entre clunas  
    //
    public static void filterPairs(Dataset<Row> newSource, Dataset<Row> tableBDW) {
        
        String[] columnsNewSource = newSource.columns();
        String[] columnsBDW = tableBDW.columns();
        ColumnProfiler cp = new ColumnProfiler();
        Cosine cosineSim = new Cosine(2);
        org.apache.commons.text.similarity.JaccardSimilarity jaccardSim = new org.apache.commons.text.similarity.JaccardSimilarity();
        JaroWinkler jaroWinklerSimilarity = new JaroWinkler();
        NormalizedLevenshtein levenshteinSimilarity = new NormalizedLevenshtein();

        for (String columnNewSource : columnsNewSource) {
            System.out.println("ColumnMain: " + columnNewSource);
            for (String columnBDW : columnsBDW) {
                if (cp.dataTypeColumn(newSource.dtypes(), columnNewSource).equals(cp.dataTypeColumn(tableBDW.dtypes(), columnBDW))) {
                    double cosinesim = cosineSim.similarity(columnNewSource, columnBDW);
                    double jaccardsim = jaccardSim.apply(columnNewSource, columnBDW);
                    double jarwinklersim = jaroWinklerSimilarity.similarity(columnNewSource, columnBDW);
                    double levenshteinSim = levenshteinSimilarity.similarity(columnNewSource, columnBDW);
                    double mean =  (cosinesim + jaccardsim + jarwinklersim + levenshteinSim) / 4;
                    System.out.println("Column " + columnNewSource + " same data type than " + columnBDW);
                    System.out.println("\t" + "---- SimilarityCosine" + "\t" + "ColumnToCompare: " + columnNewSource + "---Value: " + cosinesim );
                    System.out.println("\t" + "---- JaccardSimilarity" + "\t" + "ColumnToCompare: " + columnNewSource + "---Value: " + jaccardsim );
                    System.out.println("\t" + "---- JaroWinkler" + "\t" + "ColumnToCompare: " + columnNewSource + "---Value: " +jarwinklersim );
                    System.out.println("\t" + "---- NormalizedLevenshtein" + "\t" + "ColumnToCompare: " + columnNewSource + "---Value: " + levenshteinSim);
                    System.out.println("Mean: "  + mean);
                    System.out.println("\n");

                }

            }

        }

    }

}
