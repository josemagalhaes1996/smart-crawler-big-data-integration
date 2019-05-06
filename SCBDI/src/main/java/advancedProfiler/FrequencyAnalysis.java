/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package advancedProfiler;

import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.ListIterator;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.size;
import scala.collection.mutable.HashTable;

/**
 *
 * @author Utilizador
 */
public class FrequencyAnalysis {

    public FrequencyAnalysis() {

    }

    
    public static void main(String args[]) {
        Instant start = Instant.now();
        Connections conn = new Connections();
        Profiler prof = new Profiler("storesale_st", "customer_address", conn); //New Source
        Profiler prof2 = new Profiler("storesale_st", "customer", conn); // BDW        

        FrequencyAnalysis freqAnalysis = new FrequencyAnalysis();

        String[] columnsbdw = prof2.getDataSet().columns();
        String[] columnsNS = prof.getDataSet().columns();

        for (int i = 0; i < columnsbdw.length; i++) {
            for (int j = 0; j < columnsNS.length; j++) {

                Hashtable<String, Integer> hashtable = new Hashtable<>();
                Hashtable<String, Integer> hashfinal = new Hashtable<>();

                Dataset<Row> frequencyValues = freqAnalysis.frequencyValuesAnalysisWOLim(prof.getDataSet(), columnsNS[j]);
                Dataset<Row> frequencyValuesBDW = freqAnalysis.frequencyValuesAnalysisWOLim(prof2.getDataSet(), columnsbdw[i]);

                List<Row> rowsKey = frequencyValuesBDW.select(columnsbdw[i]).collectAsList();
                List<Row> rowVal = frequencyValuesBDW.select("count").collectAsList();
                ListIterator<Row> it = rowsKey.listIterator();
                ListIterator<Row> itVal = rowsKey.listIterator();

                while (it.hasNext() && itVal.hasNext()) {
                    try {
                        Row recordKey = it.next();
                        Row recordVal = itVal.next();

                        hashtable.put(recordKey.mkString(), Integer.parseInt(recordVal.mkString()));
                    } catch (Exception ex) {
                        System.out.println(ex.getMessage());
                    }
                }

                List<Row> rowsNSKey = frequencyValues.select(columnsNS[j]).collectAsList();
                List<Row> rowNSVal = frequencyValues.select("count").collectAsList();

                ListIterator<Row> itNS = rowsNSKey.listIterator();
                ListIterator<Row> itNSVal = rowsKey.listIterator();

                while (itNS.hasNext() && itNSVal.hasNext()) {
                    try {
                        Row record = itNS.next();
                        Row recordVal = itNSVal.next();

                        if (hashtable.containsKey(record.mkString())) {

                            int hash = hashtable.get(record.mkString()); //Tem X Valores 

                            hashfinal.put(record.mkString(), Integer.parseInt(recordVal.mkString()) * hash);
                        }

                    } catch (Exception ex) {
                        System.out.println(ex.getMessage());
                    }
                }

                
                
                int sum = 0;
                for (int f : hashfinal.values()) {
                    sum += f;
                }
                
                
                System.out.println("Atributo Main : " + columnsbdw[i] + " ");
                System.out.println("\t" + "Attribute to Compare " + columnsNS[j]);
                System.out.println("\t" + "Tamanho hasfinal: " + hashfinal.size());
                System.out.println("\t" + "Soma total de linhas AxB: " + sum);
                long intersection = (((long) hashfinal.size() * 100) / ((long) frequencyValues.count()));
                System.out.println("\t" + "Intersection: " + intersection);

            }

        }
        Instant end = Instant.now().minus(start.getEpochSecond(), ChronoUnit.SECONDS);
        System.out.println(end);

    }

    public Dataset<Row> frequencyValuesAnalysis(Dataset<Row> dataSet, String attribute) {
        return dataSet.groupBy(col(attribute)).agg(size(collect_list(attribute))
                .as("count")).select(col(attribute), col("count")).orderBy(col("count").desc()).limit(10);
    }

    public Dataset<Row> frequencyValuesAnalysisWOLim(Dataset<Row> dataSet, String attribute) {
        return dataSet.groupBy(col(attribute)).agg(size(collect_list(attribute))
                .as("count")).select(col(attribute), col("count")).orderBy(col("count").desc());
    }

}
