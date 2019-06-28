/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Encoders;
import static org.eclipse.jdt.internal.compiler.classfmt.ClassFileConstants.ClassTag;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 *
 * @author Utilizador
 */
public class SimilarityDistributed {

    public static void main(String args[]) throws IOException {
        Instant start = Instant.now();
        Connections conn = new Connections();
        similirtyAnalysis("tpcds", "promotion", "store_sales");
        Instant end = Instant.now().minus(start.getEpochSecond(), ChronoUnit.SECONDS);
        System.out.println("O JOb demorou " + end.getEpochSecond() + " Segundos");

    }

    public static void similirtyAnalysis(String dbName, String newsourceName, String sourceBDWName) {
        String db = dbName;
        String newSource = newsourceName;
        String sourceBDW = sourceBDWName;

        Connections conn = new Connections();
        Profiler prof = new Profiler(db, newSource, conn); //New Source
        Profiler prof2 = new Profiler(db, sourceBDW, conn); // BDW        

        String[] columnsbdw = prof2.getDataSet().columns();
        String[] columnsNS = prof.getDataSet().columns();

        for (int i = 0; i < columnsbdw.length; i++) {
            System.out.println("Column Main BDW: " + columnsbdw[i]);

            Dataset<Row> rowsBDWDistinct = prof2.getDataSet().select(columnsbdw[i]).distinct();

            for (int j = 0; j < columnsNS.length; j++) {
                System.out.println("Column New Source: " + columnsNS[j]);
                Dataset<Row> rowsNewSouceDistinct = prof.getDataSet().select(columnsNS[j]).distinct();

                ClassTag<Row> tag = scala.reflect.ClassTag$.MODULE$.apply(Row.class);

                RDD<Tuple2<Row, Row>> cartesianrdd = rowsBDWDistinct.rdd().cartesian(rowsNewSouceDistinct.rdd(), tag);
                JavaPairRDD<Row, Row> javapair = JavaPairRDD.fromRDD(cartesianrdd, tag, tag);

                javapair.foreach(pair -> {

                    Row row1 = pair._1;
                    Row row2 = pair._2;
                    System.out.println(row1.mkString());
                });

                Double similarityJAc = javapair.map(pair -> {

                    org.apache.commons.text.similarity.JaccardSimilarity jaccardSim = new org.apache.commons.text.similarity.JaccardSimilarity();
                    JaroWinkler jaroWinklerSimilarity = new JaroWinkler();
                    NormalizedLevenshtein levenshteinSimilarity = new NormalizedLevenshtein();

                    return jaccardSim.apply(pair._1.mkString(), pair._2.mkString());
                }).reduce(((c1, c2) -> c1 + c2));

                System.out.println("Numero de Pairs " + javapair.count());

                System.out.println("Valor da Siilaridade:" + ((double) similarityJAc / (double) javapair.count()));

                Double doubleval = rowsBDWDistinct.map((MapFunction<Row, Double>) year -> {

                    return 0.2;

                }, Encoders.DOUBLE()).reduce((c1, c2) -> c1 + c2);

                //Guardar os resultados em acumulatores...
            }
        }
    }
}
