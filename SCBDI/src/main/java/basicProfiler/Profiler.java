/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package basicProfiler;

import atlasClient.AtlasConsumer;
import JsonController.JsonControler;
import com.hortonworks.hwc.Connections;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jettison.json.JSONException;

/**
 *
 * @author Utilizador
 */
public class Profiler implements Serializable {

    private Dataset<Row> dataSet;
    private String table;
    private String database;


    /* Constructor without args */
    public Profiler() {
    }

    /* Constructor with dataset table to */
    public Profiler(String database, String table, Connections conn) {
        this.table = table;
        this.database = database;
         conn.getHiveSession().setDatabase(database);
        this.dataSet = conn.getHiveSession().executeQuery("select * from " + table);
         
        
      
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Dataset<Row> getDataSet() {
        return dataSet;
    }

    public void setDataSet(Dataset<Row> dataSet) {
        this.dataSet = dataSet;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public static void main(String[] args) throws AnalysisException, JSONException, IOException {
        Connections conn = new Connections();
        Profiler prof = new Profiler("josedb", "branch_intersect", conn);

        runcreateDataSetColumnProfiler(prof, conn.getSession());
        runcreateDataSetProfiler(prof, conn.getSession());

    }

    public static void runcreateDataSetColumnProfiler(Profiler prof, SparkSession spark) throws AnalysisException, JSONException, IOException {
        Instant start = Instant.now();
        AtlasConsumer restEntity = new AtlasConsumer();
        List<ColumnProfiler> columnProfiles = new ArrayList<>();
        ColumnProfiler profiler = new ColumnProfiler();
        for (String c1 : prof.getDataSet().columns()) {
            ColumnProfiler profilerEntity = new ColumnProfiler();
            profilerEntity = profiler.basicProfiler(prof.getDataSet(), c1, prof.getTable(), prof.getDatabase());
            columnProfiles.add(profilerEntity);
            //SendInformationToAtlas

            restEntity.createEntityAtlas(profilerEntity.getJsonColumnProfiler()); //try create!
        }
        Encoder<ColumnProfiler> columnEncoder = Encoders.bean(ColumnProfiler.class);
        Dataset<ColumnProfiler> columnProfilerDS = spark.createDataset(Collections.synchronizedList(columnProfiles), columnEncoder);
        columnProfilerDS.show();
        AtlasConsumer restconsumer = new AtlasConsumer();
        JsonControler processEntity = new JsonControler();
        Instant endDate = Instant.now();
         restconsumer.createEntity(processEntity.createEntityProcessColumnProfiler(prof.getTable(), prof.getDatabase(), start.toString(), endDate.toString(), prof.getDataSet().columns()));

    }

    public static void runcreateDataSetProfiler(Profiler prof, SparkSession spark) throws JSONException {
        Instant start = Instant.now();
        Encoder<DataSetProfiler> dataSetEncoder = Encoders.bean(DataSetProfiler.class);
        DataSetProfiler profiler = new DataSetProfiler();
        List<DataSetProfiler> dataSetProfilerList = new ArrayList<>();
        DataSetProfiler dataSetProfiler = profiler.profilerDataSet(prof.getDataSet(), prof.getTable(), prof.getDatabase());
        dataSetProfilerList.add(dataSetProfiler);
        Dataset<DataSetProfiler> dataSetProfilerDS = spark.createDataset(Collections.synchronizedList(dataSetProfilerList), dataSetEncoder);
        AtlasConsumer restconsumer = new AtlasConsumer();
        restconsumer.createEntity(dataSetProfiler.getJsonDataSetProfiler());
        JsonControler processEntity = new JsonControler();
        Instant endDate = Instant.now();
        
        restconsumer.createEntity(processEntity.createEntityProcess(prof.getTable(), prof.getDatabase(), start.toString(), endDate.toString()));
    }

}