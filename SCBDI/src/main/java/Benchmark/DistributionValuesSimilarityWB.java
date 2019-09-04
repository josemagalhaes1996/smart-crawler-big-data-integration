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
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author Utilizador
 */
public class DistributionValuesSimilarityWB {

    public static void main(String args[]) throws IOException {
        Instant start = Instant.now();
        Connections conn = new Connections();
        joinAnalysis("tpcds", "promotion", "item");
        Instant end = Instant.now().minus(start.getEpochSecond(), ChronoUnit.SECONDS);
        System.out.println("O JOb demorou " + end.getEpochSecond() + " Segundos");

    }

    public static void joinAnalysis(String dbName, String newsourceName, String sourceBDWName) throws IOException {

        String db = dbName;
        String newSource = newsourceName;
        String sourceBDW = sourceBDWName;

        Connections conn = new Connections();
        Profiler prof = new Profiler(db, newSource, conn); //New Source
        Profiler prof2 = new Profiler(db, sourceBDW, conn); // BDW        

        String[] columnsbdw = prof2.getDataSet().columns();
        String[] columnsNS = prof.getDataSet().columns();
        ArrayList<Match> matchesList = new ArrayList<>();

        for (int i = 0; i < columnsbdw.length; i++) {
            System.out.println("Column Main BDW: " + columnsbdw[i]);

            Dataset<Row> rowsBDWDistinct = prof2.getDataSet().select(columnsbdw[i]).distinct();
            Token columnbdw = new Token(columnsbdw[i]);

            for (int j = 0; j < columnsNS.length; j++) {
                System.out.println("\t" + "Column New Source: " + columnsNS[j]);
                Instant start = Instant.now();
                Token newcolumnNS = new Token(columnsNS[j]);

                Dataset<Row> rowsNewSouce = prof.getDataSet().select(columnsNS[j]);
                Broadcast<Dataset<Row>> newSourceBroadCasted = conn.getJavasparkContext().broadcast(rowsNewSouce);
                Map<Row, Long> frequencyVal = newSourceBroadCasted.value().rdd().toJavaRDD().countByValue();
                Broadcast< Map<Row, Long>> mapFrequencyBroadCasted = conn.getJavasparkContext().broadcast(frequencyVal);
                JavaRDD<Row> intersectedRows = rowsBDWDistinct.rdd().intersection(newSourceBroadCasted.value().rdd()).toJavaRDD();
                JavaRDD<Long> numValues = intersectedRows.map(x -> {

                    return mapFrequencyBroadCasted.value().get(x);

                });

                double intersection;

                if (numValues.count() > 0) {

                    long sumIntersections = numValues.reduce((c1, c2) -> c1 + c2);
                    intersection = ((double) (sumIntersections * 100)) / (double) newSourceBroadCasted.value().count();

                } else {

                    intersection = 0.0;
                }

                System.out.println("Intersection: " + intersection);
                Instant end = Instant.now().minus(start.getEpochSecond(), ChronoUnit.SECONDS);

                Score scoreIntersection = new Score(intersection, (double) end.getEpochSecond());

                Match match = new Match();
                match.setColumnBDW(columnbdw);
                match.setNewColumn(newcolumnNS);
                match.setScore(scoreIntersection);
                matchesList.add(match);

                newSourceBroadCasted.destroy();
                mapFrequencyBroadCasted.destroy();

            }
        }
                CSVGenerator.writeCSVResultsMesuresBenchMark(matchesList);

    }
}
