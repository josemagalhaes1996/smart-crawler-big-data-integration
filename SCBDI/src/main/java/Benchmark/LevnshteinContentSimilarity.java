/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Benchmark;

import Similarity.*;
import Controller.CSVGenerator;
import Domain.Match;
import Domain.Score;
import Domain.Token;
import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import info.debatty.java.stringsimilarity.Cosine;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 *
 * @author Utilizador
 */
public class LevnshteinContentSimilarity {

    public static void main(String args[]) throws IOException {
        Instant start = Instant.now();
        Connections conn = new Connections();
        similirtyAnalysis("sf1tpcds", "promotion", "store_sales");
        Instant end = Instant.now().minus(start.getEpochSecond(), ChronoUnit.SECONDS);
        System.out.println("O JOb demorou " + end.getEpochSecond() + " Segundos");

    }
   

    public static void similirtyAnalysis(String dbName, String newsourceName, String sourceBDWName) throws IOException {
      String db = dbName;
        String newSource = newsourceName;
        String sourceBDW = sourceBDWName;

        Connections conn = new Connections();
        Profiler prof = new Profiler(db, newSource, conn); //New Source
        Profiler prof2 = new Profiler(db, sourceBDW, conn); // BDW        

        String[] columnsbdw = prof2.getDataSet().columns();
        String[] columnsNS = prof.getDataSet().columns();

        ArrayList<Match> matchList = new ArrayList<>();

        for (int i = 0; i < columnsNS.length; i++) {
            System.out.println("Column New Source: " + columnsNS[i]);

            Dataset<Row> rowsNSDistinct = prof.getDataSet().select(columnsNS[i]).distinct();

            for (int j = 0; j < columnsbdw.length; j++) {
                Instant startJaccard = Instant.now();

                System.out.println("Column BDW: " + columnsbdw[j]);
                Dataset<Row> rowsBDWDistinct = prof2.getDataSet().select(columnsbdw[j]).distinct();

                ClassTag<Row> tag = scala.reflect.ClassTag$.MODULE$.apply(Row.class);

                RDD<Tuple2<Row, Row>> cartesianrdd = rowsBDWDistinct.rdd().cartesian(rowsNSDistinct.rdd(), tag);
                JavaPairRDD<Row, Row> javapair = JavaPairRDD.fromRDD(cartesianrdd, tag, tag);

                Double similarityMesure = javapair.map(pair -> {
                    NormalizedLevenshtein levenshtein= new NormalizedLevenshtein();

                    return levenshtein.similarity(pair._1.mkString(), pair._2.mkString());
                }).reduce(((c1, c2) -> c1 + c2));
                Instant endJaccard = Instant.now().minus(startJaccard.getEpochSecond(), ChronoUnit.SECONDS);

                Token tokenBDW = new Token(columnsbdw[i]);
                Token tokenNS = new Token(columnsNS[j]);
                Score scoreSimilarity = new Score(similarityMesure, (double) endJaccard.getEpochSecond());

                Match match = new Match();

                match.setColumnBDW(tokenBDW);
                match.setNewColumn(tokenNS);
                match.setScore(scoreSimilarity);

                matchList.add(match);

            }
        }

        CSVGenerator.writeCSVResultsMesuresBenchMark(matchList);
    }
}
