/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import Controller.CSVGenerator;
import advancedProfiler.FrequencyAnalysis;
import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Hashtable;
import java.util.List;
import java.util.ListIterator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.supercsv.io.ICsvListWriter;

/**
 *
 * @author Utilizador
 */
public class HashMatcher {

    public static void main(String args[]) throws IOException {
        Instant start = Instant.now();
        Connections conn = new Connections();
        intersectionAnalysis("tpcds", "promotion", "store_sales");
        Instant end = Instant.now().minus(start.getEpochSecond(), ChronoUnit.SECONDS);
        System.out.println("O JOb demorou " + end.getEpochSecond() + " Segundos");
    }

    public static void intersectionAnalysis(String dbName, String newsourceName, String sourceBDWName) throws IOException {
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
            Dataset<Row> frequencyValuesBDW = prof2.getDataSet().distinct();
            Hashtable<String, Integer> hashtable = new Hashtable<>();

            List<Row> rowsKey = prof2.getDataSet().select(columnsbdw[i]).distinct().collectAsList();
//            List<Row> rowVal = frequencyValuesBDW.select("count").collectAsList();
            System.out.println("Passou aqui");
            ListIterator<Row> it = rowsKey.listIterator();
//            ListIterator<Row> itVal = rowVal.listIterator();

//        CSVGenerator csvGenerator = new CSVGenerator();
//        ICsvListWriter listwriter = csvGenerator.createHeaderCSV("i_item_sk");
            while (it.hasNext()) {
                try {
                    Row recordKey = it.next();
                    hashtable.put(recordKey.mkString(), 0);
//                csvGenerator.writeLine(recordKey.mkString(), recordVal.mkString(), listwriter);

                } catch (Exception ex) {
                    System.out.println(ex.getMessage());
                }
            }
            System.out.println("Passou auqui vai entrar no 2º for");
            for (int j = 0; j < columnsNS.length; j++) {
                Hashtable<String, Integer> hashfinal = new Hashtable<>();
                Dataset<Row> frequencyValues = freqAnalysis.frequencyValuesAnalysisWOLim(prof.getDataSet(), columnsNS[j]);

                List<Row> rowsNSKey = frequencyValues.select(columnsNS[j]).collectAsList();
                List<Row> rowNSVal = frequencyValues.select("count").collectAsList();
                ListIterator<Row> itNS = rowsNSKey.listIterator();
                ListIterator<Row> itNSVal = rowNSVal.listIterator();

//        ICsvListWriter listwriter2 = csvGenerator.createHeaderCSV("p_promo_sk");
                while (itNS.hasNext() && itNSVal.hasNext()) {
                    try {
                        Row record = itNS.next();
                        Row recordVal = itNSVal.next();
//                csvGenerator.writeLine(record.mkString(), recordVal.mkString(), listwriter2);

                        Integer entityCheck = hashtable.get(record.mkString());

                        if (entityCheck != null) {

                            int generatedRows = Integer.parseInt(recordVal.mkString());
//                            System.out.println("Tem estes valores " + generatedRows);

                            hashtable.remove(record.mkString());
                            hashfinal.put(record.mkString(), generatedRows);
                        } else {
//                            System.out.println("Não encontrou este par no hashtable: " + record.mkString());
                        }
                    } catch (Exception ex) {
                        System.out.println(ex.getMessage());
                    }
                }

                int sumGeneratedRows = 0;

                for (int value : hashfinal.values()) {
                    sumGeneratedRows = sumGeneratedRows + value;
                }

                System.out.println("Numero de Linhas Geradas: " + sumGeneratedRows);
                System.out.println("Numero de Linhas DataSet New Source: " + prof.getDataSet().count());

                double percentFillTable = (((double) (sumGeneratedRows * 100)) / ((double) prof.getDataSet().count()));

//            Hashtable<String, String> pairs = new Hashtable<>();
//            pairs.put(columnsbdw[i], columnsNS[j]);
//            hashIntersection.put(percentFillTable, pairs);
                System.out.println("Atributo Main: " + columnsbdw[i] + " Atributo New: " + columnsNS[j]);
                System.out.println("\t" + " GeneratedRows: " + sumGeneratedRows + " Percent Fill: " + percentFillTable);

            }

//        
//        //Filtering Pairs 
//        long sumIntersections = 0;
//        long meanintersections = 0;
//        Set<Long> setKeys = hashIntersection.keySet();
//        for (long keyval : setKeys) {
//            sumIntersections = sumIntersections + keyval;
//        }
//        meanintersections = ((long) sumIntersections) / ((long) hashIntersection.size());
//
//        for (long keyval : setKeys) {
//            if (keyval < meanintersections) {
//            } else {
//                Hashtable<String, String> finalPair = hashIntersection.get(keyval);
//                Map.Entry<String, String> entry = finalPair.entrySet().iterator().next();
//                String mainAttribute = entry.getKey();
//                String attributeToCompare = entry.getValue();
//                System.out.println("PairMain: " + mainAttribute + " Attribute to Compare: " + attributeToCompare + "\t" + " Intersection: " + keyval);
//            }
//        }
//  
//        System.out.println("\t" + "MeanIntersection: " + meanintersections);
        }
    }

    public static double hashMatcherSimilarity(String columnBDW, String columnNewDataSource, Dataset<Row> bdw, Dataset<Row> newSource, Dataset<Row> frequencyValuesBDWs, Dataset<Row> frequencyValuesNSs) throws IOException {

        FrequencyAnalysis freqAnalysis = new FrequencyAnalysis();
        Dataset<Row> frequencyValuesBDW = frequencyValuesBDWs;

        Hashtable<String, Integer> hashtable = new Hashtable<>();

        List<Row> rowsKey = frequencyValuesBDW.select(columnBDW).collectAsList();
        List<Row> rowVal = frequencyValuesBDW.select("count").collectAsList();

        ListIterator<Row> it = rowsKey.listIterator();
        ListIterator<Row> itVal = rowVal.listIterator();

        CSVGenerator csvGenerator = new CSVGenerator();
        ICsvListWriter listwriter = csvGenerator.createHeaderCSV(columnBDW);

        while (it.hasNext() && itVal.hasNext()) {
            try {
                Row recordKey = it.next();
                Row recordVal = itVal.next();
                hashtable.put(recordKey.mkString(), Integer.parseInt(recordVal.mkString()));
                csvGenerator.writeLine(recordKey.mkString(), recordVal.mkString(), listwriter);

            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        }

        Hashtable<String, Integer> hashfinal = new Hashtable<>();
        Dataset<Row> frequencyValues = frequencyValuesNSs;

        List<Row> rowsNSKey = frequencyValues.select(columnNewDataSource).collectAsList();
        List<Row> rowNSVal = frequencyValues.select("count").collectAsList();
        ListIterator<Row> itNS = rowsNSKey.listIterator();
        ListIterator<Row> itNSVal = rowNSVal.listIterator();
        ICsvListWriter listwriter2 = csvGenerator.createHeaderCSV(columnNewDataSource);

        while (itNS.hasNext() && itNSVal.hasNext()) {
            try {
                Row record = itNS.next();
                Row recordVal = itNSVal.next();
                csvGenerator.writeLine(record.mkString(), recordVal.mkString(), listwriter2);

                if (hashtable.containsKey(record.mkString())) { //Como já tinhamos, posteriormente podiamos remover esse valor!
                    int hash = hashtable.get(record.mkString()); //Tem X Valores 
                    int generatedRows = Integer.parseInt(recordVal.mkString());
                    hashfinal.put(record.mkString(), generatedRows);

                } else {
//                    System.out.println("Não encontrou este par no hashtable: " + record.mkString());
                }
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        }
//                }
        int sumGeneratedRows = 0;

        for (int value : hashfinal.values()) {
            sumGeneratedRows = sumGeneratedRows + value;
        }

        System.out.println("Numero de Linhas Geradas: " + sumGeneratedRows);
        System.out.println("Numero de Linhas DataSet New Source: " + newSource.count());
        double percentFillTable = (((double) (sumGeneratedRows * 100)) / ((double) newSource.count()));

//                pairs.put(columnsbdw[i], columnsNS[j]);
//                hashIntersection.put(intersection, pairs);
        System.out.println("Atributo Main: " + columnBDW + " Atributo New: " + columnNewDataSource);
        System.out.println("\t" + " GeneratedRows: " + sumGeneratedRows + " Percent Fill: " + percentFillTable);

        return percentFillTable;
    }

}
