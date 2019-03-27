/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import com.hortonworks.hwc.Connections;
import basicProfiler.Profiler;
import java.io.Serializable;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.size;

import static org.apache.spark.sql.functions.levenshtein;
import org.apache.commons.text.similarity.*;

/**
 *
 * @author Utilizador
 */
public class Tests implements Serializable {

    public static void main(String args[]) {

        Connections conn = new Connections();
        Profiler prof = new Profiler("tpcds", "store_sales", conn);

        prof.getDataSet().show();
        //  prof.getDataSet().show();
        // prof.getDataSet().printSchema();
        // prof.getDataSet().describe().show();
        // prof.getDataSet().describe("coffee_bar").show();
        // Dataset<Row> col = prof.getDataSet().select(col("first_opened_date"));
//
//        Tuple2<String, String>[] dtype = prof.getDataSet().dtypes();
//        String dtypecolumn = null;
//
//        for (Tuple2<String, String> tuplearray : dtype) {
//            if (tuplearray._1().equalsIgnoreCase("coffee_bar")) {
//                dtypecolumn = tuplearray._2();
//                break;
//            }
//        }
//
//        List<Row> listmaxDate = col.withColumn("first_opened_date", col("first_opened_date")).agg(org.apache.spark.sql.functions.max(col("first_opened_date"))).collectAsList();
//        Timestamp tempo = Timestamp.valueOf(listmaxDate.get(0).mkString());
//
//        ArrayList<String> stringarray = new ArrayList<>();
//        stringarray.add("2.0");
//        stringarray.add("3.1");
//
//        //teste para converter em array de doubles...
//        Profiler prof2 = new Profiler("foodmart", "account", conn);
//        Dataset<Row> col2 = prof2.getDataSet().select(col("account_id"));
//        List<Row> listdouble = col2.select("account_id").collectAsList();
//
//        double[] doublelist = new double[listdouble.size()];
//        for (int i = 0; i < listdouble.size(); i++) {
//
//            doublelist[i] = Double.parseDouble(listdouble.get(i).mkString());
//
//        }
//
//        //  JavaSparkContext sc = new JavaSparkContext(conn.getConf());
//        //tem de ter o mesmo tamanho o vetor..
//        Vector dv = org.apache.spark.mllib.linalg.Vectors.dense(doublelist);
//        Vector dv2 = org.apache.spark.mllib.linalg.Vectors.dense(doublelist);
//        List<Vector> dist = new ArrayList<>();
//        dist.add(dv);
//        dist.add(dv2);
//        JavaRDD<Vector> rdd = conn.getJavasparkContext().parallelize(dist);
//
//        RowMatrix mat = new RowMatrix(rdd.rdd());
//        CoordinateMatrix cord = mat.columnSimilarities();
//        List<Vector> sims = mat.columnSimilarities().toRowMatrix().rows().toJavaRDD().collect();
//        JavaRDD<Vector> javarddresult = mat.columnSimilarities().toRowMatrix().rows().toJavaRDD();
//
//        //Fazer For e percorrer string e criar um array de doubles..
//        Dataset<Row> frequencyDataSet = prof.getDataSet().groupBy(col("first_opened_date")).agg(size(collect_list("first_opened_date")).as("count")).select(col("first_opened_date"), col("count")).orderBy(col("count").desc()).limit(10);
//
//        frequencyDataSet.select("first_opened_date").collectAsList().get(0);
//        frequencyDataSet.select("count").collectAsList().get(0);
//        frequencyDataSet.show();
//
//        List<String> data = new ArrayList<String>();
//        data.add(tempo.toString());
//        data.add(dtypecolumn);
//        data.add(stringarray.toString());
//        data.add(listdouble.toString());
//        data.add(listdouble.get(1).mkString());
//        data.add(String.valueOf(mat.columnSimilarities().entries().first().value()));
//        data.add(sims.get(0).toString());
////        data.add(doublelist.toString());
//        for(int i = 0 ; i< frequencyDataSet.collectAsList().size();i++){
//        
//        data.add(frequencyDataSet.select("first_opened_date").collectAsList().get(i).mkString());
//        data.add(frequencyDataSet.select("count").collectAsList().get(i).mkString());
//        }

//
//        Dataset<Row> df = conn.getSession().createDataset(data, Encoders.STRING()).toDF();
//        df.show();
//// DataFrame
        //Distance in same table //Essta parte já está retratada em metodo numa classe ( Semantic Similarity9
//        Dataset<Row> df2 = prof2.getDataSet().withColumn("LevenshteinDistance", levenshtein(col("account_id"), col("account_id")));
//        df2.show();
//       JaccardDistance n = new JaccardDistance(); 
//       
//       n.apply("teste", "teste44");
//    
//       JaccardSimilarity sim = new JaccardSimilarity();
//       
//       sim.apply("shop", "store");
    }

}
