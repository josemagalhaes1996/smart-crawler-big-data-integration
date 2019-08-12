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
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author Utilizador
 */
public class SimilarityMesuresProcessingTimeHeaders {

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

                Instant startCosine = Instant.now();
                double cosinesim = cosineSim.similarity(columnNewSource, columnBDW);
                Instant endCosine = Instant.now();

                Instant startJaccard = Instant.now();
                double jaccardsim = jaccardSim.similarity(columnNewSource, columnBDW);
                Instant endJaccard = Instant.now();

                Instant startJaroWinkler = Instant.now();
                double jarowinklersim = jaroWinklerSimilarity.similarity(columnNewSource, columnBDW);
                Instant endJaroWinkler = Instant.now();

                Instant startLevenshtein = Instant.now();
                double levenshteinSim = levenshteinSimilarity.similarity(columnNewSource, columnBDW);
                Instant endLevenshtein = Instant.now();

                Score score = new Score(Duration.between(startJaccard, endJaccard).toMillis(), Duration.between(startJaroWinkler, endJaroWinkler).toMillis(), Duration.between(startLevenshtein, endLevenshtein).toMillis(), Duration.between(startCosine, endCosine).toMillis());

                System.out.println("\t" + "---- CosineSimilarity" + "\t" + Duration.between(startCosine, endCosine).toMillis());
                System.out.println("\t" + "---- JaccardSimilarity" + "\t" + Duration.between(startJaccard, endJaccard).toMillis());
                System.out.println("\t" + "---- JaroWinkler" + "\t" + Duration.between(startJaroWinkler, endJaroWinkler).toMillis());
                System.out.println("\t" + "---- Levenshtein" + "\t" + Duration.between(startLevenshtein, endLevenshtein).toMillis());
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
