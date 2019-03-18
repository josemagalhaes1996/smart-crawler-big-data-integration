/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hortonworks.hwc;

import com.hortonworks.hwc.HiveWarehouseSession;
import static com.hortonworks.hwc.HiveWarehouseSession.*;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import scala.Tuple2;

/**
 *
 * @author Utilizador
 */
public class Connections implements Serializable {

    private SparkConf conf;
    private SparkSession session;
    private JavaSparkContext javasparkContext;

    private HiveWarehouseSession hiveSession;

    public Connections() {

        this.conf = new SparkConf().setAppName("ProfilerApp").setMaster("master").set("spark.driver.allowMultipleContexts", "true");
        this.session = SparkSession.builder().appName("ProfilerApp")
                .enableHiveSupport().getOrCreate();
        this.javasparkContext = JavaSparkContext.fromSparkContext(this.session.sparkContext());
        this.hiveSession = com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder.session(this.session).build();
        
        
    }
    
 public String dataTypeColumn(Tuple2<String, String>[] dtype, String columnName) {
        String dtypecolumn = null;
        for (Tuple2<String, String> tuplearray : dtype) {
            if (tuplearray._1().equalsIgnoreCase(columnName)) {
                dtypecolumn = tuplearray._2();
                break;
            }
        }
        return (dtypecolumn);
    }
 
    public static void main(String args[]) {
        Connections conn = new Connections();
        conn.getHiveSession().showDatabases().show();
        conn.getHiveSession().setDatabase("josedb");
 
      Dataset<Row> ds =  conn.getHiveSession().table("brancha");
     Dataset<Row> col = ds.select(col("full_name"));
       List<Row> listmaxVal = col.agg(org.apache.spark.sql.functions.max(col("full_name"))).collectAsList();

       
        String datatypes = conn.dataTypeColumn(ds.dtypes(), "full_name");

        //if dataType is String, minValue and maxValue functions results are null;
        String minValue = null;
        String maxValue = null;
        String maxFieldLength = null;
        String minFieldLength = null;
        long truevaluecount = 0;
        long falsevaluecount = 0;
        long emptyvalues = 0;
        if (datatypes.equalsIgnoreCase("StringType") || datatypes.equalsIgnoreCase("TimestampType") || datatypes.equalsIgnoreCase("BooleanType")) {
            minValue = null;
            maxValue = null;

        } else {
            Dataset< Row> describeData = ds.describe("full_name");
            List<Row> obj = describeData.select("full_name").collectAsList();
            minValue = obj.get(3).mkString();
            maxValue = obj.get(4).mkString();
        }
        // If the type is TimesTamp;
        if (datatypes.equalsIgnoreCase("TimestampType") || datatypes.equalsIgnoreCase("BooleanType")) {
            if (datatypes.equalsIgnoreCase("BooleanType")) {
                Dataset<Row> trueValuesDataSet = ds.filter(col("full_name").equalTo(true));
                Dataset<Row> falseValuesDataSet = ds.filter(col("full_name").equalTo(false));
                truevaluecount = trueValuesDataSet.count();
                falsevaluecount = falseValuesDataSet.count();
            } else {
                List<Row> listmaxDate = col.withColumn("maxDate", col("full_name")).agg(org.apache.spark.sql.functions.max(col("full_name"))).collectAsList();
                Timestamp maxtime = Timestamp.valueOf(listmaxDate.get(0).mkString());
                maxValue = maxtime.toString();
                List<Row> listminDate = col.withColumn("minDate", col("full_name")).agg(org.apache.spark.sql.functions.min(col("full_name"))).collectAsList();
                Timestamp mintime = Timestamp.valueOf(listminDate.get(0).mkString());
                minValue = mintime.toString();
            }
        } else {
           
            maxFieldLength = String.valueOf(listmaxVal.get(0).mkString().length());
            List<Row> listminVal = col.withColumn("fieldleng", col("full_name")).agg(org.apache.spark.sql.functions.min(col("full_name"))).collectAsList();
            minFieldLength = String.valueOf(listminVal.get(0).mkString().length());
            emptyvalues = col.withColumn("isEmpty", col("full_name").isNaN()).filter(col("isEmpty").equalTo(true)).count();

        }
        long recordCounts = col.count();
        long uniqueValue = col.distinct().count();
        long nullvalues = col.withColumn("isNull", col("full_name").isNull()).filter(col("isNull").equalTo(true)).count();
     
       
    }

    public HiveWarehouseSession getHiveSession() {
        return hiveSession;
    }

    public void setHiveSession(HiveWarehouseSession hiveSession) {
        this.hiveSession = hiveSession;
    }

    public JavaSparkContext getJavasparkContext() {
        return javasparkContext;
    }

    public void setJavasparkContext(JavaSparkContext javasparkContext) {
        this.javasparkContext = javasparkContext;
    }

    public SparkConf getConf() {
        return conf;
    }

    public void setConf(SparkConf conf) {
        this.conf = conf;
    }

    public SparkSession getSession() {
        return session;
    }

    public void setSession(SparkSession session) {
        this.session = session;
    }

}
