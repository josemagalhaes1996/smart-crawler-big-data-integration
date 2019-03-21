/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package basicProfiler;

import JsonController.JsonControler;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.SizeEstimator;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author Utilizador
 */
public class DataSetProfiler implements Serializable {

    private String tableName;
    private long size;
    private int numVariables;
    private long numObservations;
    private int numericColumns;
    private int categoricalColumns;
    private int dateColumns;
    private int othersColmuns;
    private JSONObject jsonDataSetProfiler;

    public DataSetProfiler(String name, long size, int numVariables, long numObservations, int numericColumns, int categoricalColumns, int dateColumns, int othersColmuns, JSONObject jsonDataSetProfiler) {
        this.tableName = name;
        this.size = size;
        this.numVariables = numVariables;
        this.numObservations = numObservations;
        this.numericColumns = numericColumns;
        this.categoricalColumns = categoricalColumns;
        this.dateColumns = dateColumns;
        this.othersColmuns = othersColmuns;
        this.jsonDataSetProfiler = jsonDataSetProfiler;
    }

    
    public JSONObject getJsonDataSetProfiler() {
        return jsonDataSetProfiler;
    }

    public void setJsonDataSetProfiler(JSONObject jsonDataSetProfiler) {
        this.jsonDataSetProfiler = jsonDataSetProfiler;
    }

    public DataSetProfiler() {
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public int getNumVariables() {
        return numVariables;
    }

    public void setNumVariables(int numVariables) {
        this.numVariables = numVariables;
    }

    public long getNumObservations() {
        return numObservations;
    }

    public void setNumObservations(long numObservations) {
        this.numObservations = numObservations;
    }

    public int getNumericColumns() {
        return numericColumns;
    }

    public void setNumericColumns(int numericColumns) {
        this.numericColumns = numericColumns;
    }

    public int getCategoricalColumns() {
        return categoricalColumns;
    }

    public void setCategoricalColumns(int categoricalColumns) {
        this.categoricalColumns = categoricalColumns;
    }

    public int getDateColumns() {
        return dateColumns;
    }

    public void setDateColumns(int dateColumns) {
        this.dateColumns = dateColumns;
    }

    public int getOthersColmuns() {
        return othersColmuns;
    }

    public void setOthersColmuns(int othersColmuns) {
        this.othersColmuns = othersColmuns;
    }

    public DataSetProfiler profilerDataSet(Dataset<Row> dataSet, String tableName,String database) throws JSONException {
        String tbName = tableName;
        int numColumn = dataSet.columns().length;
        long numObservations = dataSet.count();
        long datasetSize = SizeEstimator.estimate(dataSet);
        int numNumericColumns = 0;
        int numCategoricalColumns = 0;
        int numDataColumns = 0;
        int numOtherColumns = 0;

        ColumnProfiler datatypesColumn = new ColumnProfiler();
        List<ColumnProfiler> columnProfiles = new ArrayList<>();
        for (String c1 : dataSet.columns()) {
            ColumnProfiler profiler = new ColumnProfiler();
            String dtype = profiler.dataTypeColumn(dataSet.dtypes(), c1);
            if (dtype.equalsIgnoreCase("IntegerType") || dtype.equalsIgnoreCase("DoubleType")
                    || dtype.equalsIgnoreCase("FloatType") || dtype.equalsIgnoreCase("LongType")) {
                numNumericColumns = numNumericColumns + 1;
            } else if (dtype.equalsIgnoreCase("StringType") || dtype.equalsIgnoreCase("BooleanType")) {
                numCategoricalColumns = numCategoricalColumns + 1;

            } else if (dtype.equalsIgnoreCase("DateType") || dtype.equalsIgnoreCase("TimestampType")) {
                numDataColumns = numDataColumns + 1;
            } else {
                numOtherColumns = numOtherColumns + 1;
            }
        }

        JsonControler jsonClass = new JsonControler();
        JSONObject jsonEntity= jsonClass.createEntityTableProfiler(database, tableName, numCategoricalColumns, numDataColumns, (int) numObservations, numColumn, numNumericColumns, numOtherColumns,(int)datasetSize);
     
        DataSetProfiler columnnew = new DataSetProfiler(tbName, datasetSize, numColumn, numObservations, numNumericColumns, numCategoricalColumns, numDataColumns, numOtherColumns, jsonEntity);
        return (columnnew);
    }

}
