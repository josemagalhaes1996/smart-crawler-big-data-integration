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

    public static void writeHeadersCSV(ArrayList<Match> matchArray) throws IOException {

        ICsvListWriter listWriter = null;
        try {
            listWriter = new CsvListWriter(new FileWriter("./FilterBenchmarkResults.csv"),
                    CsvPreference.STANDARD_PREFERENCE);

            final CellProcessor[] processors = getProcessors();
            final String[] header = new String[]{"New Source Column Name", "BDW Column Name", "Jaccard Similarity", "Cosine Similarity", "Jaro-Winkler Similarity", "Levenshtein Similarity",
                "JCN", "WUP", "PATH"};

            // write the header
            listWriter.writeHeader(header);

            // write the customer lists
            if (matchArray.size() > 0) {
                for (Match match : matchArray) {
                    Score score = match.getScore();

                    if (score.getConstructor() == 1) {

                        listWriter.write(Arrays.asList(new Object[]{match.getNewColumn(), match.getColumnBDW(), match.getScore().getJaccard(), match.getScore().getCosine(),
                            match.getScore().getJaro_winkler(), match.getScore().getLevenshetein(), "", "", "", match.getScore().getAverageAll()}), processors);

                    } else {

                        listWriter.write(Arrays.asList(new Object[]{match.getNewColumn(), match.getColumnBDW(), "", "", "", "", match.getScore().getJiangandConrath(), match.getScore().getWu_Palmer(), match.getScore().getPATH(), match.getScore().getAverageAll()}), processors);
                    }

                }

            }

        } finally {
            if (listWriter != null) {
                listWriter.close();
            }
        }

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
}
