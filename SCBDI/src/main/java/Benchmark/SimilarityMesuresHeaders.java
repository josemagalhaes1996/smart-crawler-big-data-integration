/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Benchmark;

import Controller.CSVGenerator;
import Domain.Match;
import Domain.Score;
import Domain.Token;
import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import info.debatty.java.stringsimilarity.Cosine;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author Utilizador
 */
public class SimilarityMesuresHeaders {

    public static void main(String args[]) throws IOException {
        Connections conn = new Connections();
        Profiler prof = new Profiler("tpcds", "store_sales", conn);
        Profiler prof2 = new Profiler("tpcds", "income_band", conn);
        
        String path = "/user/jose/storesale_er/promotion/promotion.csv";
        String delimiter = ";";
        String header = "true";
        Dataset<Row> newDataset = conn.getSession().read().format("csv").option("header", header).option("delimiter", delimiter).option("inferSchema", "true").load(path);
        
            filterPairs(prof2.getDataSet(), prof.getDataSet());
    }

    public static void filterPairs(Dataset<Row> newSource, Dataset<Row> tableBDW) throws IOException {
        String[] columnsNewSource = newSource.columns();
        String[] columnsBDW = tableBDW.columns();

        Cosine cosineSim = new Cosine(2);
        info.debatty.java.stringsimilarity.Jaccard jaccardSim = new info.debatty.java.stringsimilarity.Jaccard();
        JaroWinkler jaroWinklerSimilarity = new JaroWinkler();
        NormalizedLevenshtein levenshteinSimilarity = new NormalizedLevenshtein();

        ArrayList<Match> matchesList = new ArrayList<>();

        for (String columnNewSource : columnsNewSource) {
            System.out.println("\n" + " ColumnMain: " + columnNewSource);

            Token newcolumnToken = new Token(columnNewSource);

            for (String columnBDW : columnsBDW) {

                Token columnTokenBDW = new Token(columnBDW);

                double cosinesim = cosineSim.similarity(columnNewSource, columnBDW);
                double jaccardsim = jaccardSim.similarity(columnNewSource, columnBDW);
                double jarowinklersim = jaroWinklerSimilarity.similarity(columnNewSource, columnBDW);
                double levenshteinSim = levenshteinSimilarity.similarity(columnNewSource, columnBDW);

                Score score = new Score(jaccardsim, jarowinklersim, levenshteinSim, cosinesim);

                System.out.println("\t" + "---- CosineSimilarity" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + cosinesim);
                System.out.println("\t" + "---- JaccardSimilarity" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + jaccardsim);
                System.out.println("\t" + "---- JaroWinkler" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + jarowinklersim);
                System.out.println("\t" + "---- Levenshtein" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + levenshteinSim);
                System.out.println("Mean: " + score.getAverageSimilarity());
                System.out.println("\n");

                Match match = new Match();
                match.setColumnBDW(columnTokenBDW);
                match.setNewColumn(newcolumnToken);
                match.setScore(score);
                matchesList.add(match);

            }
        }

        CSVGenerator.writeCSVResults(matchesList);
        System.out.println("Number of Pairs " + matchesList.size());
    }
}
