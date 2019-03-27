/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hortonworks.hwc;

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
