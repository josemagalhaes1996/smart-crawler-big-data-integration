/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import advancedProfiler.FrequencyAnalysis;
import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Hashtable;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author Utilizador
 */
public class HashMatcher {

    public static void main(String args[]) {
        Instant start = Instant.now();
        Connections conn = new Connections();
        intersectionAnalyss("storesale_er", "promotion", "store_sales");
        Instant end = Instant.now().minus(start.getEpochSecond(), ChronoUnit.SECONDS);
        System.out.println(end.getEpochSecond());
    }

    public static void intersectionAnalyss(String dbName, String newsourceName, String sourceBDWName) {
        String db = dbName;
        String newSource = newsourceName;
        String sourceBDW = sourceBDWName;

        Connections conn = new Connections();
        Profiler prof = new Profiler(db, newSource, conn); //New Source
        Profiler prof2 = new Profiler(db, sourceBDW, conn); // BDW        

        FrequencyAnalysis freqAnalysis = new FrequencyAnalysis();
        String[] columnsbdw = prof2.getDataSet().columns();
        String[] columnsNS = prof.getDataSet().columns();

        Hashtable<Long, Hashtable<String, String>> hashIntersection = new Hashtable<>();

        for (int i = 0; i < columnsbdw.length; i++) {

            Dataset<Row> frequencyValuesBDW = freqAnalysis.frequencyValuesAnalysisWOLim(prof2.getDataSet(), columnsbdw[i]);
            Hashtable<String, Integer> hashtable = new Hashtable<>();
            List<Row> rowsKey = frequencyValuesBDW.select(columnsbdw[i]).collectAsList();
            List<Row> rowVal = frequencyValuesBDW.select("count").collectAsList();
            ListIterator<Row> it = rowsKey.listIterator();
            ListIterator<Row> itVal = rowVal.listIterator();

            while (it.hasNext() && itVal.hasNext()) {
                try {
                    Row recordKey = it.next();
                    Row recordVal = itVal.next();
                    hashtable.put(recordKey.mkString(), Integer.parseInt(recordVal.mkString()));
                } catch (Exception ex) {
                    System.out.println(ex.getMessage());
                }
            }
            for (int j = 0; j < columnsNS.length; j++) {

                Hashtable<String, Integer> hashfinal = new Hashtable<>();
                Dataset<Row> frequencyValues = freqAnalysis.frequencyValuesAnalysisWOLim(prof.getDataSet(), columnsNS[j]);

                List<Row> rowsNSKey = frequencyValues.select(columnsNS[j]).collectAsList();
                List<Row> rowNSVal = frequencyValues.select("count").collectAsList();
                ListIterator<Row> itNS = rowsNSKey.listIterator();
                ListIterator<Row> itNSVal = rowNSVal.listIterator();

                while (itNS.hasNext() && itNSVal.hasNext()) {
                    try {
                        Row record = itNS.next();
                        Row recordVal = itNSVal.next();

                        if (hashtable.containsKey(record.mkString())) {
                            int hash = hashtable.get(record.mkString()); //Tem X Valores 
                            System.out.println(hash);
                            int generatedRows = Integer.parseInt(recordVal.mkString());
                            hashfinal.put(record.mkString(), hash);
                        }
                    } catch (Exception ex) {
                        System.out.println(ex.getMessage());
                    }
                }
                int sumGeneratedRows = 0;
                for (int value : hashfinal.values()) {
                    sumGeneratedRows = sumGeneratedRows + value;
                }
                long percentFillTable = ((sumGeneratedRows / prof2.getDataSet().count()) * 100);
                long intersection = (((long) hashfinal.size() * 100) / ((long) frequencyValues.count()));
                Hashtable<String, String> pairs = new Hashtable<>();
                pairs.put(columnsbdw[i], columnsNS[j]);
                hashIntersection.put(intersection, pairs);
                System.out.println("Atributo Main: " + columnsbdw[i] + " Atributo New: " + columnsNS[j]);
                System.out.println("\t" + "Intersection: " + intersection + " GeneratedRows: " + sumGeneratedRows + " Percent Fill: " + percentFillTable);

            }
        }
        //Filtering Pairs 
        long sumIntersections = 0;
        long meanintersections = 0;
        Set<Long> setKeys = hashIntersection.keySet();
        for (long keyval : setKeys) {
            sumIntersections = sumIntersections + keyval;
        }
        meanintersections = ((long) sumIntersections) / ((long) hashIntersection.size());

        for (long keyval : setKeys) {
            if (keyval < meanintersections) {
            } else {
                Hashtable<String, String> finalPair = hashIntersection.get(keyval);
                Map.Entry<String, String> entry = finalPair.entrySet().iterator().next();
                String mainAttribute = entry.getKey();
                String attributeToCompare = entry.getValue();
                System.out.println("PairMain: " + mainAttribute + " Attribute to Compare: " + attributeToCompare + "\t" + " Intersection: " + keyval);
            }
        }
        System.out.println("\t" + "MeanIntersection: " + meanintersections);

    }
}
