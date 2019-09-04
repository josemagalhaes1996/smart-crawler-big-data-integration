/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controller;

import Domain.Match;
import Domain.Score;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

/**
 *
 * @author Utilizador
 */
public class CSVGenerator {

    public CSVGenerator() {
    }

    /**
     *
     *
     * @param matchArray
     * @throws IOException
     */
    public static void writeCSVResults(ArrayList<Match> matchArray) throws IOException {

        ICsvListWriter listWriter = null;
        try {
            listWriter = new CsvListWriter(new FileWriter("./FilterBenchmarkResults.csv"),
                    CsvPreference.STANDARD_PREFERENCE);

            final CellProcessor[] processors = getMesuresProcessors();
            final String[] header = new String[]{"Pairs", "Jaccard Similarity", "Jaro-Winkler Similarity", "Levenshtein Similarity", "Cosine Similarity"};

            // write the header
            listWriter.writeHeader(header);

            // write the customer lists
            if (matchArray.size() > 0) {
                for (Match match : matchArray) {
                    Score score = match.getScore();

                    if (score.getConstructor() == 1) {

                        listWriter.write(Arrays.asList(new Object[]{match.getNewColumn().getToken() + "-" + match.getColumnBDW().getToken(), match.getScore().getJaccard(), match.getScore().getCosine(),
                            match.getScore().getJaro_winkler(), match.getScore().getLevenshetein()}), processors);

                    } else {

                        listWriter.write(Arrays.asList(new Object[]{match.getNewColumn().getToken(), match.getColumnBDW().getToken(), "", "", "", "", match.getScore().getJiangandConrath(), match.getScore().getWu_Palmer(), match.getScore().getPATH(), match.getScore().getAverageSimilarity()}), processors);
                    }

                }

            }

        } finally {
            if (listWriter != null) {
                listWriter.close();
            }
        }

    }

    public static void writeCSVResults2(ArrayList<Match> matchArray) throws IOException {

        ICsvListWriter listWriter = null;
        try {
            listWriter = new CsvListWriter(new FileWriter("./BenchmarkContent.csv"),
                    CsvPreference.STANDARD_PREFERENCE);

            final CellProcessor[] processors = getContentProcessors();
            final String[] header = new String[]{
                //                "New Source Column Name", 
                "Pairs",
                "Jaccard Similarity", "Jaccard  Time",
                "Cosine Similarity", "Cosine  Time",
                "Jaro-Winkler Similarity", "Jaro-Winkler Time",
                "Levenshtein Similarity", "Levenshtein Time",
                "HashSimilarity", "Hash Time",
                "AverageSimilarity", "AverageTime"};

            // write the header
            listWriter.writeHeader(header);

            // write the customer lists
            if (matchArray.size() > 0) {
                for (Match match : matchArray) {
                    Score score = match.getScore();
                    if (score.getConstructor() == 3) {
                        listWriter.write(Arrays.asList(new Object[]{match.getNewColumn().getToken() + "-" + match.getColumnBDW().getToken(),
                            match.getScore().getJaccard(), match.getScore().getJaccardTime(),
                            match.getScore().getCosine(), match.getScore().getCosineTime(),
                            match.getScore().getJaro_winkler(), match.getScore().getJaro_winklerTime(),
                            match.getScore().getLevenshetein(), match.getScore().getLevensheteinTime(),
                            match.getScore().getHashMatcher(), match.getScore().getHashTime(),
                            match.getScore().getAverageSimilarity(), match.getScore().getAverageTime()}), processors);
                    }
                }
            }
        } finally {
            if (listWriter != null) {
                listWriter.close();
            }
        }

    }

    public static void writeCSVJaccard(ArrayList<Match> matchArray) throws IOException {

        ICsvListWriter listWriter = null;
        try {
            listWriter = new CsvListWriter(new FileWriter("./ResultsJaccard.csv"),
                    CsvPreference.STANDARD_PREFERENCE);

            final CellProcessor[] processors = getMesuresProcessors();
            final String[] header = new String[]{
                //                "New Source Column Name", 
                "Pairs",
                "Jaccard Similarity", "Jaccard  Time",};

            // write the header
            listWriter.writeHeader(header);

            // write the customer lists
            if (matchArray.size() > 0) {
                for (Match match : matchArray) {
                    Score score = match.getScore();
                    if (score.getConstructor() == 3) {
                        listWriter.write(Arrays.asList(new Object[]{match.getNewColumn().getToken() + "-" + match.getColumnBDW().getToken(),
                            match.getScore().getJaccard(), match.getScore().getJaccardTime()}));
                    }
                }
            }
        } finally {
            if (listWriter != null) {
                listWriter.close();
            }
        }

    }
      public static void writeCSVResultsMesuresBenchMark(ArrayList<Match> matchArray) throws IOException {

        ICsvListWriter listWriter = null;
        try {
            listWriter = new CsvListWriter(new FileWriter("./BenchmarkContentBenchmark.csv"),
                    CsvPreference.STANDARD_PREFERENCE);

            final CellProcessor[] processors = getProcessorsContentMesures();
            final String[] header = new String[]{
                //                "New Source Column Name", 
                "Pairs",
                "Mesure Similarity", "Time Processing"};

            // write the header
            listWriter.writeHeader(header);

            // write the customer lists
            if (matchArray.size() > 0) {
                for (Match match : matchArray) {
                    Score score = match.getScore();
                    if (score.getConstructor() == 4) {
                        listWriter.write(Arrays.asList(new Object[]{match.getNewColumn().getToken() + "-" + match.getColumnBDW().getToken(),
                            match.getScore().getIntersection(), match.getScore().getProcessingTime()}), processors);
                    }
                    if(score.getConstructor() == 5){
                     listWriter.write(Arrays.asList(new Object[]{match.getNewColumn().getToken() + "-" + match.getColumnBDW().getToken(),
                            match.getScore().getCosine(),""}), processors);
                    }
                }
            }
        } finally {
            if (listWriter != null) {
                listWriter.close();
            }
        }

    }

    public ICsvListWriter createHeaderCSV(String name) throws IOException {
        ICsvListWriter listWriter = null;
        try {
            listWriter = new CsvListWriter(new FileWriter("./" + name + ".csv"),
                    CsvPreference.STANDARD_PREFERENCE);

            final String[] header = new String[]{
                //                "New Source Column Name", 
                "ColumnValue", "FrequencyValue"};

            // write the header
            listWriter.writeHeader(header);

        } finally {

        }

        return listWriter;
    }

    public void writeLine(String name, String frequencyValues, ICsvListWriter listWriter) throws IOException {

        final CellProcessor[] processors = getProcessorsFrequencyValues();

        listWriter.write(Arrays.asList(new Object[]{name, frequencyValues}), processors);

    }

    private static CellProcessor[] getProcessors() {
        final CellProcessor[] processors = new CellProcessor[]{
            new Optional(), //New Souce Column Name  
            new Optional(), // BDW Column Name
            new Optional(), //Jaccard 
            new Optional(), //Cosine 
            new Optional(), //Jaro-Winkler
            new Optional(), // Levenshtein
            new Optional(), // JCN
            new Optional(), //WUP
            new Optional(), //PATH
            new Optional(), //Average All Similarities
        };

        return processors;
    }

    private static CellProcessor[] getProcessorsFrequencyValues() {
        final CellProcessor[] processors = new CellProcessor[]{
            new Optional(), // ColumnName 
            new Optional(), // FrequencyValues
        };

        return processors;
    }

    private static CellProcessor[] getContentProcessors() {
        final CellProcessor[] processors = new CellProcessor[]{
            new Optional(), //New Souce Column Name  
            //            new Optional(), // BDW Column Name
            new Optional(), //Jaccard 
            new Optional(), //Jaccard Time
            new Optional(), //Cosine 
            new Optional(), //Cosine  Time
            new Optional(), //Jaro-Winkler
            new Optional(), //Jaro-Winkler Time
            new Optional(), // Levenshtein
            new Optional(), // Levenshtein Time
            new Optional(), // HASH SIMILARITY
            new Optional(), // HASH Time
            new Optional(), // AverageSimilarity
            new Optional(), // AverageTime
        };

        return processors;
    }

    private static CellProcessor[] getMesuresProcessors() {
        final CellProcessor[] processors = new CellProcessor[]{
            new Optional(), //Pairs
            new Optional(), //Jaccard 
            new Optional(), //Levenshtein
            new Optional(), //Cosine
            new Optional(), //Jaro-Winkler
        };

        return processors;
    }

    private static CellProcessor[] getProcessorsContentMesures() {
        final CellProcessor[] processors = new CellProcessor[]{
            new Optional(), //Pairs
            new Optional(), //Mesure 
            new Optional(), //Time Processing 
        };

        return processors;
    }
}
