/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import Controller.CSVGenerator;
import Domain.Match;
import Domain.Score;
import Domain.Token;
import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import hesmlclient.HESMLclient;
import info.debatty.java.stringsimilarity.Cosine;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author Utilizador
 */
public class FilterPairs {

    public static void main(String args[]) throws IOException {
        Connections conn = new Connections();
//        Profiler prof = new Profiler("tpcds", "store_sales", conn);
//        String path = "/user/jose/storesale_er/promotion/promotion.csv";
        String path = "/user/jose/Genoma/GWAS_dataset.csv";

        String path2 = "/user/jose/Genoma/DisGeNET RAW.csv";
        String delimiter = ";";
        String header = "true";
        Dataset<Row> newDataset = conn.getSession().read().format("csv").option("header", header).option("delimiter", delimiter).option("inferSchema", "true").load(path);
        Dataset<Row> newDataset2 = conn.getSession().read().format("csv").option("header", header).option("delimiter", delimiter).option("inferSchema", "true").load(path2);

        filterPairs(newDataset, newDataset2, 0, true);

    }

    public static void filterPairs(Dataset<Row> newSource, Dataset<Row> tableBDW, double threshold, boolean wordNetProcess) throws IOException {
        String[] columnsNewSource = newSource.columns();
        String[] columnsBDW = tableBDW.columns();

        Cosine cosineSim = new Cosine(2);

        ArrayList<Match> matchesList = new ArrayList<>();

        for (String columnNewSource : columnsNewSource) {
            System.out.println("\n" + " ColumnMain: " + columnNewSource);

            Token newcolumnToken = new Token(columnNewSource);

            for (String columnBDW : columnsBDW) {

                Token columnTokenBDW = new Token(columnBDW);

                double cosinesim = cosineSim.similarity(columnNewSource, columnBDW);

                System.out.println("\t" + "---- CosineSimilarity" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + cosinesim);
                System.out.println("\n");

                Match match = new Match();

                //FALTA VERIFICAR QUE SER OU NAO UTILIZAR A SEMANTICA 
                /*  CASO o valor Seja inferior da média for inferior a 0.6 será aplicado ontologias de dados*/
                /* client , customer - sex - gender */
                /* Devido às ontologias é necessário algumas combinações nos dados*/
                if (cosinesim < threshold) {
                    Double ontologyScore = null;
                    if (wordNetProcess == true) {
                        ontologyScore = checkOntology(columnNewSource, columnBDW, threshold);
                    }

                    if (!(ontologyScore == null)) {
                        Score score = new Score(ontologyScore);
                        match.setColumnBDW(columnTokenBDW);
                        match.setNewColumn(newcolumnToken);
                        match.setScore(score);
                        matchesList.add(match);
                    }
                } else {
                    Score score = new Score(cosinesim);
                    match.setColumnBDW(columnTokenBDW);
                    match.setNewColumn(newcolumnToken);
                    match.setScore(score);
                    matchesList.add(match);
                }
            }
        }
        CSVGenerator.writeCSVResultsMesuresBenchMark(matchesList);
        System.out.println("Number of Pairs " + matchesList.size());

        
        
    }
    
    

    public static Double checkOntology(String newcolumnToken, String columnTokenBDW, double threshold) {

        ArrayList<Double> semanticScoreList = new ArrayList<>();
        Double bestscore = null;
        hesmlclient.HESMLclient semanticClient = new HESMLclient();
        ArrayList<String> stringNewSource = new ArrayList<>();
        ArrayList<String> stringSourceBDW = new ArrayList<>();
        double[] resultsSemanctic = null;
        String camelCase = "(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])";

        try {

            resultsSemanctic = semanticClient.semanticPairSimilarity(columnTokenBDW, newcolumnToken);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        if (resultsSemanctic == null) {
            System.out.println("Split Process||| CamelCase|| By - || By _ || By ");
            //Split word by - and _ and CamelCase ... 3 different situations
            if (newcolumnToken.contains("-")) {
                System.out.println("Split by - ");
                String[] splitedColumnName = newcolumnToken.split("-");
                for (String stringHifen : splitedColumnName) {
                    stringNewSource.add(stringHifen);
                }

            } else if (newcolumnToken.contains("_")) {
                System.out.println("Split by _ ");
                String[] splitedColumnName = newcolumnToken.split("_");
                for (String stringUnderScore : splitedColumnName) {
                    stringNewSource.add(stringUnderScore);
                }

            } else if (camelCase.matches(newcolumnToken)) {
                System.out.println("Split by camelCase ");
                for (String stringCamelCase : newcolumnToken.split("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])")) {
                    stringNewSource.add(stringCamelCase);
                }
            }

            if (columnTokenBDW.contains("-")) {
                System.out.println("Split by - ");
                String[] splitedColumnName = columnTokenBDW.split("-");
                for (String stringHifen : splitedColumnName) {
                    stringSourceBDW.add(stringHifen);
                }

            } else if (columnTokenBDW.contains("_")) {
                System.out.println("Split by _ ");
                String[] splitedColumnName = columnTokenBDW.split("_");
                for (String stringUnderScore : splitedColumnName) {
                    stringSourceBDW.add(stringUnderScore);
                }
            } else if (camelCase.matches(columnTokenBDW)) {
                System.out.println("Split by camelCase ");
                for (String stringCamelCase : columnTokenBDW.split("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])")) {
                    stringSourceBDW.add(stringCamelCase);
                }
            }

            stringNewSource.stream().forEach((token) -> {

                stringSourceBDW.stream().forEach((tokenbdw) -> {
                    try {

                        double[] result = semanticClient.semanticPairSimilarity(token, tokenbdw);

                        if (!(result == null)) {

                            semanticScoreList.add(result[0]);
                        }

                    } catch (Exception e) {
                        e.getMessage();
                    }
                });
            });

            if (semanticScoreList.size() > 0) {

                for (Double scoreElement : semanticScoreList) {

                    if (bestscore == null) {
                        bestscore = scoreElement;
                    } else {
                        if (bestscore > scoreElement) {
                            bestscore = scoreElement;
                        }
                    }
                }

            } else {
                System.out.println("\n" + "Score List is empty");
                return null;
            }

            if (bestscore > threshold) {
                System.out.println("\n" + "Recorreu ao passos semanticos e obteve uma similaridade de " + bestscore);

                return bestscore;

            }

        } else {
            bestscore = resultsSemanctic[0];
            System.out.println("Semanticamente " + bestscore);

            if (bestscore > threshold) {
                System.out.println("\n" + " Semantic Similarity Score  is " + bestscore);
                return bestscore;
            }
        }

        return bestscore;
    }

//Falta preencher os exceis e separar os metodos da ontologia e os outros ! 
}
