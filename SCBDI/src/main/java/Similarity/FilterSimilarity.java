/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import Domain.Match;
import Domain.Score;
import Domain.Token;
import basicProfiler.ColumnProfiler;
import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import hesmlclient.HESMLclient;
import info.debatty.java.stringsimilarity.Cosine;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
import java.util.ArrayList;
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
        String header = "true";
        Dataset<Row> dataset = conn.getSession().read().format("csv").option("header", header).option("delimiter", delimiter).option("inferSchema", "true").load(path);
        firstFilterPairs(dataset, prof.getDataSet(), 0.6);
    }

    
    public static void firstFilterPairs(Dataset<Row> newSource, Dataset<Row> tableBDW, double threshold) {
        String[] columnsNewSource = newSource.columns();
        String[] columnsBDW = tableBDW.columns();

        Cosine cosineSim = new Cosine(2);
        org.apache.commons.text.similarity.JaccardSimilarity jaccardSim = new org.apache.commons.text.similarity.JaccardSimilarity();
        JaroWinkler jaroWinklerSimilarity = new JaroWinkler();
        NormalizedLevenshtein levenshteinSimilarity = new NormalizedLevenshtein();
        ArrayList<Match> matchesList = new ArrayList<>();

        for (String columnNewSource : columnsNewSource) {
            System.out.println("\n" + " ColumnMain: " + columnNewSource);

            Token newcolmnToken = new Token(columnNewSource);
            for (String columnBDW : columnsBDW) {
                Token columnTokenBDW = new Token(columnBDW);

                double cosinesim = cosineSim.similarity(columnNewSource, columnBDW);
                double jaccardsim = jaccardSim.apply(columnNewSource, columnBDW);
                double jarowinklersim = jaroWinklerSimilarity.similarity(columnNewSource, columnBDW);
                double levenshteinSim = levenshteinSimilarity.similarity(columnNewSource, columnBDW);

                Score score = new Score(jaccardsim, jarowinklersim, levenshteinSim, cosinesim);

                System.out.println("\t" + "---- SimilarityCosine" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + cosinesim);
                System.out.println("\t" + "---- JaccardSimilarity" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + jaccardsim);
                System.out.println("\t" + "---- JaroWinkler" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + jarowinklersim);
                System.out.println("\t" + "---- Levenshtein" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + levenshteinSim);
                System.out.println("Mean: " + score.getAverageAll());
                System.out.println("\n");

                /*  CASO o valor Seja inferior da m�dia for inferior a 0.6 ser� aplicado ontologias de dados*/
                /* client , customer - sex - gender */
                /* Devido �s ontologias � necess�rio algumas combina��es nos dados*/
                if (score.getAverageAll() < threshold) {

                    System.out.println("M�dia de Similaridade Inferior a  " + threshold);
                    ArrayList<Score> semanticScoreList = new ArrayList<>();
                    Score bestscore = null;
                    hesmlclient.HESMLclient semanticClient = new HESMLclient();
                    ArrayList<String> stringNewSource = new ArrayList<>();
                    ArrayList<String> stringSourceBDW = new ArrayList<>();
                    double[] resultsSemanctic = null;

                    String camelCase = "(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])";

                    try {

                        resultsSemanctic = semanticClient.semanticPairSimilarity(columnBDW, columnNewSource);

                    } catch (Exception e) {
                        System.out.println("entrou logo auqi");
                        System.out.println(e.getMessage());
                    }

                    if (resultsSemanctic == null) {
                        System.out.println("Split Process||| CamelCase|| By - || By _ || By ");
                        //Split word by - and _ and CamelCase ... 3 different situations
                        if (columnNewSource.contains("-")) {
                            String[] splitedColumnName = columnNewSource.split("-");
                            for (String stringHifen : splitedColumnName) {
                                stringNewSource.add(stringHifen);
                            }

                        } else if (columnNewSource.contains("_")) {
                            String[] splitedColumnName = columnNewSource.split("_");
                            for (String stringUnderScore : splitedColumnName) {
                                stringNewSource.add(stringUnderScore);
                            }

                        } else if (camelCase.matches(columnNewSource)) {
                            for (String stringCamelCase : columnNewSource.split("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])")) {
                                stringNewSource.add(stringCamelCase);
                            }
                        }

                        if (columnBDW.contains("-")) {
                            String[] splitedColumnName = columnBDW.split("-");
                            for (String stringHifen : splitedColumnName) {
                                stringSourceBDW.add(stringHifen);
                            }

                        } else if (columnBDW.contains("_")) {
                            String[] splitedColumnName = columnBDW.split("_");
                            for (String stringUnderScore : splitedColumnName) {
                                stringSourceBDW.add(stringUnderScore);
                            }
                        } else if (camelCase.matches(columnBDW)) {
                            for (String stringCamelCase : columnBDW.split("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])")) {
                                stringSourceBDW.add(stringCamelCase);
                            }
                        }

                        stringNewSource.stream().forEach((token) -> {
                            stringSourceBDW.stream().forEach((tokenbdw) -> {
                                try {

                                    double[] result = semanticClient.semanticPairSimilarity(token, tokenbdw);

                                    if (!(result == null)) {

                                        Score semanticScore = new Score(result[0], result[1], result[2]);
                                        semanticScoreList.add(semanticScore);
                                    }

                                } catch (Exception e) {
                                    e.getMessage();
                                }
                            });
                        });

                        if (semanticScoreList.size() > 0) {

                            for (Score scoreElement : semanticScoreList) {

                                if (bestscore == null) {
                                    bestscore = scoreElement;
                                } else {
                                    if (bestscore.getAverageAll() > scoreElement.getAverageAll()) {
                                        bestscore = scoreElement;
                                    }
                                }
                            }

                        } else {
                            System.out.println("\n" + "SCORE List is empty");
                        }

                        if (bestscore.getAverageAll() > threshold) {
                            System.out.println("\n" + "Recorreu ao passos semanticos e obteve uma similaridade de " + bestscore.getAverageAll());

                            Match match = new Match();
                            match.setColumnBDW(columnTokenBDW);
                            match.setNewColumn(newcolmnToken);
                            match.setScore(bestscore);
                            matchesList.add(match);
                        } else {
                            System.out.println("\n" + "ScoreList Semantic is " + bestscore.getAverageAll());
                        }

                    } else {
                        System.out.println("first step only");
                        bestscore = new Score(resultsSemanctic[0], resultsSemanctic[1], resultsSemanctic[2]);
                        System.out.println("Semanticamente " + bestscore.getAverageAll());
                        System.out.println("Elemetno 1 " + resultsSemanctic[0]);
                        if (bestscore.getAverageAll() > threshold) {
                            System.out.println("\n" + "Recorreu ao passos semanticos e obteve uma similaridade de " + bestscore.getAverageAll());
                            System.out.println("\n" + "Best Score Semantic Similirty value is " + bestscore.getAverageAll());

                            Match match = new Match();
                            match.setColumnBDW(columnTokenBDW);
                            match.setNewColumn(newcolmnToken);
                            match.setScore(bestscore);
                            matchesList.add(match);
                        }
                    }
                } else {
                    Match match = new Match();
                    match.setColumnBDW(columnTokenBDW);
                    match.setNewColumn(newcolmnToken);
                    match.setScore(score);
                    matchesList.add(match);
                }
            }
        }

        System.out.println("Number of Emparelhamentos: " + matchesList.size());
    }

//    public Score semanticFilter() {
//
//    }
}
