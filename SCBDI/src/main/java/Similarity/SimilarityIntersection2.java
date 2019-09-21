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
import static Similarity.SimilarityIntersection.joinAnalysis;
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
public class SimilarityIntersection2 {
    public static void main(String args[]) throws IOException {
        Instant start = Instant.now();
        joinAnalysis("tpcds", "promotion", "store_sales");
        Instant end = Instant.now().minus(start.getEpochSecond(), ChronoUnit.SECONDS);
        System.out.println("O JOb demorou " + end.getEpochSecond() + " Segundos");

    }
    
    public static void joinAnalysis(String dbName, String newsourceName, String sourceBDWName) throws IOException {
        String db = dbName;
        String newSource = newsourceName;
        String sourceBDW = sourceBDWName;

        Connections conn = new Connections();
//        Profiler prof = new Profiler(db, newSource, conn); //New Source
//        Profiler prof2 = new Profiler(db, sourceBDW, conn); // BDW        
//       String[] columnsbdw = prof2.getDataSet().columns();
//        String[] columnsNS = prof.getDataSet().columns();

        String path = "/user/jose/Genoma/DisGeNET RAW.csv";
        String path2 = "/user/jose/Genoma/Ensembl_dataset.csv";
        String delimiter = ";";
        String header = "true";
        Dataset<Row> newDataset = conn.getSession().read().format("csv").option("header", header).option("delimiter", delimiter).option("inferSchema", "true").load(path);
        Dataset<Row> newDataset2 = conn.getSession().read().format("csv").option("header", header).option("delimiter", delimiter).option("inferSchema", "true").load(path2);
        
        System.out.println("Linhas de BDW:"+ newDataset.count()  );
        System.out.println("Linhas de new DataSource:"+ newDataset2.count()  );

        
        String[] columnsbdw = newDataset.columns();
        String[] columnsNS = newDataset2.columns();
        
        ArrayList<Match> matchesList = new ArrayList<>();

        for (int i = 0; i < columnsNS.length; i++) {

            
            System.out.println("Column New Source: " + columnsNS[i]);
            Dataset<Row> rowsNSDistinct = newDataset2.select(columnsNS[i]).distinct();
            rowsNSDistinct = rowsNSDistinct.where(rowsNSDistinct.col(columnsNS[i]).isNotNull()); //delete nulls
            Token newcolumnNS = new Token(columnsNS[i]);
        
//            rowsNSDistinct.show(); //Está correto
            
            
            for (int j = 0; j < columnsbdw.length; j++) {
                
                
                Instant start = Instant.now();
                Token columnbdw = new Token(columnsbdw[j]);
                System.out.println("\t" + "Column BDW: " + columnsbdw[j]);
              
                Dataset<Row> rowsBDW = newDataset.select(columnsbdw[j]);
            //    System.out.println("Numero de linhas com nulls: " + rowsBDW.count());
                
                rowsBDW = rowsBDW.where(rowsBDW.col(columnsbdw[j]).isNotNull()); //delete nulls
               
              //  System.out.println("Numero de linhas sem nulls: " + rowsBDW.count());

                Map<Row, Long> frequencyVal = rowsBDW.rdd().toJavaRDD().countByValue();

                               
                JavaRDD<Row> intersectedRows = rowsNSDistinct.intersect(rowsBDW).rdd().toJavaRDD();
              
                System.out.println("Número de linhas intersectadas:" + intersectedRows.count());
                
                
                JavaRDD<Long> numValues = intersectedRows.map(x -> {    
                
                    return frequencyVal.get(x);

                });
                
                
                double intersection = 0;
                
                
                
                if (numValues.count() > 0) {

                    numValues.toString();
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

        CSVGenerator.writeCSVResultsMesuresBenchMark(matchesList);
    }

}
