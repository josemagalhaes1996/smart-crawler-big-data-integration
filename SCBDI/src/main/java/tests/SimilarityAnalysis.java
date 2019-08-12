/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import Controller.CSVGenerator;
import Domain.Match;
import Domain.Score;
import Domain.Token;
import advancedProfiler.FrequencyAnalysis;
import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import info.debatty.java.stringsimilarity.Cosine;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import scala.Function2;

/**
 *
 * @author Utilizador
 *
 */
public class SimilarityAnalysis {

    /**
     *
     * Analysis between two different tables Intra and Inter Analysis attributes
     * Mesures: Jaccard,Cosine, Levenshtein, Jaro-Winkler
     */
    public static void main(String[] args) throws IOException {
        Connections conn = new Connections();
        Profiler prof = new Profiler("tpcds", "item", conn);
//        Profiler prof2 = new Profiler("storesale_st", "household_demographics", conn);

        List<String> out = new ArrayList<>();
        String path = "/user/jose/storesale_er/promotion/promotion.csv";
        String delimiter = ";";
        String header = "true";
        Dataset<Row> newDataSource = conn.getSession().read().format("csv").option("header", header).option("delimiter", delimiter).option("inferSchema", "true").load(path);

//        runSimilarityIntraColumn(conn, prof2.getDataSet(), prof2.getDataSet().columns(), out, 2, 0, prof2.getDataSet().columns().length);
        runSimilarityInterAnalysis(conn, newDataSource, prof.getDataSet());

    }

    public static void runSimilarityInterAnalysis(Connections conn, Dataset<Row> newDataSource, Dataset<Row> BDWSource) throws IOException {
        System.out.println("Linhas são " + BDWSource.count());
      
      Cosine cosineSim = new Cosine(1);
        org.apache.commons.text.similarity.JaccardSimilarity jaccardSim = new org.apache.commons.text.similarity.JaccardSimilarity();
        JaroWinkler jaroWinklerSimilarity = new JaroWinkler();
        NormalizedLevenshtein levenshteinSimilarity = new NormalizedLevenshtein();
        ArrayList<Match> matchList = new ArrayList<>();
        FrequencyAnalysis freqAnalysis = new FrequencyAnalysis();

        for (String columnNewSource : newDataSource.columns()) {

            Dataset<Row> frequencyValuesNS = freqAnalysis.frequencyValuesAnalysisWOLim(newDataSource, columnNewSource);
            List<Row> newSourceRows = newDataSource.select(columnNewSource).distinct().collectAsList();
            System.out.println("\n" + " ColumnMain: " + columnNewSource);

            for (String columnBDW : BDWSource.columns()) {

                Dataset<Row> frequencyValuesBDW = freqAnalysis.frequencyValuesAnalysisWOLim(BDWSource, columnBDW);

                List<Row> BDWRows = BDWSource.select(columnBDW).distinct().collectAsList();
                Instant startCosine = Instant.now();
//                System.out.println("\t" + "---- SimilarityCosine" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + cosineSim.similarity(newSourceRows.toString(), BDWRows.toString()));
                double cosineSimilarity = cosineSim.similarity(newSourceRows.toString(), BDWRows.toString());
                Instant endCosine = Instant.now();
                Duration timeCosine = Duration.between(startCosine, endCosine);

                Instant startJaccard = Instant.now();
//                System.out.println("\t" + "---- JaccardSimilarity" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + jaccardSim.apply(newSourceRows.toString(), BDWRows.toString()));
                double jaccardSimilarity = jaccardSim.apply(newSourceRows.toString(), BDWRows.toString());
                Instant endJaccard = Instant.now();
                Duration timeJaccard = Duration.between(startJaccard, endJaccard);

                Instant startJaroWinkler = Instant.now();
//                System.out.println("\t" + "---- JaroWinkler" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + jaroWinklerSimilarity.similarity(newSourceRows.toString(), BDWRows.toString()));
                double JaroWinklerSimilarity = jaroWinklerSimilarity.similarity(newSourceRows.toString(), BDWRows.toString());
                Instant endJaroWinkler = Instant.now();
                Duration timeJaroWinkler = Duration.between(startJaroWinkler, endJaroWinkler);

                Instant startLevenshtein = Instant.now();
//                System.out.println("\t" + "---- NormalizedLevenshtein" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + levenshteinSimilarity.similarity(newSourceRows.toString(), BDWRows.toString()));
                double levenshteinSim = levenshteinSimilarity.similarity(newSourceRows.toString(), BDWRows.toString());
                Instant endLevenshtein = Instant.now();
                Duration timeLevenshtein = Duration.between(startLevenshtein, endLevenshtein);

                Instant startHash = Instant.now();
                double hashSimilarity = HashMatcher.hashMatcherSimilarity(columnBDW, columnNewSource, BDWSource, newDataSource, frequencyValuesBDW, frequencyValuesNS);

                Instant endHash = Instant.now();
                Duration timeHash = Duration.between(startHash, endHash);

                Token tokenNewSource = new Token(columnNewSource);
                Token tokenBDW = new Token(columnBDW);
                Match match = new Match();
                match.setColumnBDW(tokenBDW);
                match.setNewColumn(tokenNewSource);
                Score score = new Score(jaccardSimilarity, (double) timeJaccard.getNano() / 1000000000, JaroWinklerSimilarity, (double) timeJaroWinkler.getNano() / 1000000000, levenshteinSim, (double) timeLevenshtein.getNano() / 1000000000, cosineSimilarity, (double) timeCosine.getNano() / 1000000000, hashSimilarity, (double) timeHash.getNano() / 1000000000);
                match.setScore(score);
                matchList.add(match);
            }
        }
        CSVGenerator.writeCSVResults2(matchList);

    }

}
