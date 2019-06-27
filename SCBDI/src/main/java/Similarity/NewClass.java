/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import advancedProfiler.FrequencyAnalysis;
import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.spark.sql.Row;

/**
 *
 * @author Utilizador
 */
public class NewClass {

    public static void main(String args[]) throws IOException {
        Instant start = Instant.now();
        Connections conn = new Connections();
        integrationWithOn2("tpcds", "promotion", "store_sales");
        Instant end = Instant.now().minus(start.getEpochSecond(), ChronoUnit.SECONDS);
        System.out.println("O JOb demorou " + end.getEpochSecond() + " Segundos");

    }

    public static void integrationWithOn2(String dbName, String newsourceName, String sourceBDWName) throws IOException {
        String db = dbName;
        String newSource = newsourceName;
        String sourceBDW = sourceBDWName;

        Connections conn = new Connections();
        Profiler prof = new Profiler(db, newSource, conn); //New Source
        Profiler prof2 = new Profiler(db, sourceBDW, conn); // BDW        

        FrequencyAnalysis freqAnalysis = new FrequencyAnalysis();
        String[] columnsbdw = prof2.getDataSet().columns();
        String[] columnsNS = prof.getDataSet().columns();
        int sum = 0;

        for (int i = 0; i < columnsbdw.length; i++) {

            List<Row> bdwRows = prof2.getDataSet().select(columnsbdw[i]).distinct().collectAsList();

            for (int j = 0; j < columnsNS.length; j++) {
                sum = 0;
                List<Row> newSourceRows = prof.getDataSet().select(columnsNS[j]).collectAsList();

                for (int k = 0; k < bdwRows.size(); k++) {

                    for (int m = 0; m < newSourceRows.size(); m++) {

                        if (bdwRows.get(k).mkString().equalsIgnoreCase(newSourceRows.get(m).mkString())) {

                            sum = sum + 1;
                        }

                    }

                }
                double intersection = (((double) sum * 100) / (double) prof.getDataSet().count());

                System.out.println("Intersection between  " + columnsbdw[i] + " e " + columnsNS[j] + " é: " + intersection);

            }

        }

    }

}
