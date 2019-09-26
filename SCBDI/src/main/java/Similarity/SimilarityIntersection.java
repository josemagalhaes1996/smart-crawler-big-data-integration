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
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author Utilizador
 */
public class SimilarityIntersection {

    public static void main(String args[]) throws IOException {
        Instant start = Instant.now();
        joinAnalysis("sf3tpcds", "promotion", "store_sales");
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
        
//
//        String path = "/user/jose/Genoma/AlzForum_dataset.csv";
//        String path2 = "/user/jose/Genoma/DisGeNET.csv";
//        String delimiter = ";";
//        String header = "true";
//        Dataset<Row> newDataset = conn.getSession().read().format("csv").option("header", header).option("delimiter", delimiter).load(path);
//        Dataset<Row> newDataset2 = conn.getSession().read().format("csv").option("header", header).option("delimiter", delimiter).load(path2);
//
//        System.out.println("Linhas de BDW:" + newDataset.count());
////        System.out.println("Linhas de new DataSource:" + newDataset2.count());
//
//        String[] columnsbdw = newDataset.columns();
//        String[] columnsNS = newDataset2.columns();

        ArrayList<Match> matchesList = new ArrayList<>();

        for (int i = 0; i < columnsNS.length; i++) {

            System.out.println("Column New Source: " + columnsNS[i]);
            Dataset<Row> rowsNSDistinct = prof.getDataSet().select(columnsNS[i]).distinct();
            rowsNSDistinct = rowsNSDistinct.where(rowsNSDistinct.col(columnsNS[i]).isNotNull()); //delete nulls
            Token newcolumnNS = new Token(columnsNS[i]);

//              rowsNSDistinct.show(); //Está correto
            for (int j = 0; j < columnsbdw.length; j++) {

                Instant start = Instant.now();
                Token columnbdw = new Token(columnsbdw[j]);
                System.out.println("\t" + "Column BDW: " + columnsbdw[j]);

                Dataset<Row> rowsBDW =  prof2.getDataSet().select(columnsbdw[j]);

                rowsBDW = rowsBDW.where(rowsBDW.col(columnsbdw[j]).isNotNull()); //delete nulls

                Map<Row, Long> frequencyVal = rowsBDW.toJavaRDD().countByValue();

//              System.out.println(frequencyVal.toString());
                JavaRDD<Row> intersectedRows = rowsNSDistinct.intersect(rowsBDW).rdd().toJavaRDD();

//              System.out.println("Número de linhas intersectadas:" + intersectedRows.count()); //DEu certo
//                System.out.println("Intersecção  numeros:" + intersectedRows.collect().toString());

           if (intersectedRows.count() > 0) {
                    JavaRDD<Long> numValues = intersectedRows.map(x -> {

                        return frequencyVal.get(x);

                    });

                    double intersection = 0;

                    if (numValues.count() > 0) {
//                        System.out.println("Values to Sum:" + numValues.collect().toString());

                        long sumIntersections = numValues.reduce((c1, c2) -> c1 + c2);
                        System.out.println("SumIntersections: " + sumIntersections);
                        intersection = ((double) (sumIntersections * 100)) / (double) rowsBDW.count();
                    } else {

                        intersection = 0.0;

                    }

                    DecimalFormat df = new DecimalFormat("#.00");
                    intersection = Double.parseDouble(df.format(intersection));

                    System.out.println("Intersection: " + intersection);
                    Instant end = Instant.now().minus(start.getEpochSecond(), ChronoUnit.SECONDS);
                    Score scoreIntersection = new Score(intersection, (double) end.getEpochSecond());
                    Match match = new Match();
                    match.setColumnBDW(columnbdw);
                    match.setNewColumn(newcolumnNS);
                    match.setScore(scoreIntersection);
                    matchesList.add(match);

                }

            }
        }

        CSVGenerator.writeCSVResultsMesuresBenchMark(matchesList);
    }
    

    public static double similarityInterface(Dataset<Row> sourceBDW, Dataset<Row> sourceNS, String columnbdw, String columnNS) throws IOException {
        Dataset<Row> rowsBDWDistinct = sourceBDW.select(columnbdw).distinct();
        rowsBDWDistinct = rowsBDWDistinct.where(rowsBDWDistinct.col(columnbdw).isNotNull()); //delete nulls

        Dataset<Row> rowsNewSouce = sourceNS.select(columnNS);
        
        rowsNewSouce = rowsNewSouce.where(rowsNewSouce.col(columnNS).isNotNull()); //delete nulls
        Map<Row, Long> frequencyVal = rowsNewSouce.rdd().toJavaRDD().countByValue();

        JavaRDD<Row> intersectedRows = rowsBDWDistinct.rdd().intersection(rowsNewSouce.rdd()).toJavaRDD();
     
        JavaRDD<Long> numValues = intersectedRows.map(x -> {
         
            return frequencyVal.get(x);
            
        });
        double intersection = 0;

        if (numValues.count() > 0) {
            long sumIntersections = numValues.reduce((c1, c2) -> c1 + c2);
            intersection = ((double) (sumIntersections * 100)) / (double) rowsNewSouce.count();
        } else {
            intersection = 0.0;
        }

        DecimalFormat df = new DecimalFormat("#.00");
        intersection = Double.parseDouble(df.format(intersection));
        return intersection;
    }

}
