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
        filterPairs(dataset, prof.getDataSet());
    }

    public static void firstFilterPairs(Dataset<Row> newSource, Dataset<Row> tableBDW) {

        String[] columnsNewSource = newSource.columns();
        String[] columnsBDW = tableBDW.columns();

        Cosine cosineSim = new Cosine(2);
        org.apache.commons.text.similarity.JaccardSimilarity jaccardSim = new org.apache.commons.text.similarity.JaccardSimilarity();
        JaroWinkler jaroWinklerSimilarity = new JaroWinkler();
        NormalizedLevenshtein levenshteinSimilarity = new NormalizedLevenshtein();
        ArrayList<Match> matchesList = new ArrayList<>();

        for (String columnNewSource : columnsNewSource) {
            System.out.println("ColumnMain: " + columnNewSource);

            
            Token newcolmn = new Token(columnNewSource);
            for (String columnBDW : columnsBDW) {
                Token columnTokenBDW = new Token(columnBDW);

                double cosinesim = cosineSim.similarity(columnNewSource, columnBDW);
                double jaccardsim = jaccardSim.apply(columnNewSource, columnBDW);
                double jarowinklersim = jaroWinklerSimilarity.similarity(columnNewSource, columnBDW);
                double levenshteinSim = levenshteinSimilarity.similarity(columnNewSource, columnBDW);
                double mean = (cosinesim + jaccardsim + jarowinklersim + levenshteinSim) / 4;

                Score score = new Score(jaccardsim, jarowinklersim, levenshteinSim, cosinesim);

                System.out.println("\t" + "---- SimilarityCosine" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + cosinesim);
                System.out.println("\t" + "---- JaccardSimilarity" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + jaccardsim);
                System.out.println("\t" + "---- JaroWinkler" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + jarowinklersim);
                System.out.println("\t" + "---- NormalizedLevenshtein" + "\t" + "ColumnToCompare: " + columnBDW + "---Value: " + levenshteinSim);
                System.out.println("Mean: " + mean);
                System.out.println("\n");

                /*  CASO o valor Seja inferior da média for inferior a 0 ( nada em comum) , será aplicado ontologias de dados*/
                /* client , customer - sex - gender */
                /* Devido às ontologias é necessário algumas combinações nos dados*/
                //Word1  falta o 1o if
                
                if (mean < 0.6) {
                    columnBDW.replace(" ", "");
                    columnNewSource.replace(" ", "");
                    String camelCase = "(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])";
                    ArrayList<String> stringNewSource = new ArrayList<>();
                    ArrayList<String> stringSourceBDW = new ArrayList<>();

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
                    
                    for (String token : stringNewSource) {
                        for (String tokenbdw : stringSourceBDW) {
                        try{
                        
                        }catch(Exception e){
                        
                        }
                        }
                    }

                } else {
                    Match match = new Match();
                    match.setColumnBDW(columnTokenBDW);
                    match.setNewColumn(newcolmn);
                    match.setScore(score);
                    matchesList.add(match);
                }
            }
        }

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
                    double mean = (cosinesim + jaccardsim + jarwinklersim + levenshteinSim) / 4;
                    System.out.println("Column " + columnNewSource + " same data type than " + columnBDW);
                    System.out.println("\t" + "---- SimilarityCosine" + "\t" + "ColumnToCompare: " + columnNewSource + "---Value: " + cosinesim);
                    System.out.println("\t" + "---- JaccardSimilarity" + "\t" + "ColumnToCompare: " + columnNewSource + "---Value: " + jaccardsim);
                    System.out.println("\t" + "---- JaroWinkler" + "\t" + "ColumnToCompare: " + columnNewSource + "---Value: " + jarwinklersim);
                    System.out.println("\t" + "---- NormalizedLevenshtein" + "\t" + "ColumnToCompare: " + columnNewSource + "---Value: " + levenshteinSim);
                    System.out.println("Mean: " + mean);
                    System.out.println("\n");

                }

            }

        }

    }

}
