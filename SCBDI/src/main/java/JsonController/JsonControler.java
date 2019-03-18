/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package JsonController;

/**
 *
 * @author Utilizador
 */
import atlasClient.AtlasConsumer;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.codehaus.jettison.json.JSONException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class JsonControler {

    public JsonControler() {
    }

    public JSONObject createEntityColumnProfiler(String columnName, String datatype, String database, String tablename,
            String comment, String min, String max, long recordCount, long uniqueValues, long emptyValues, long nullValues, String maxFieldLength, String minFieldLength, long percentFillRecords, long percentUniqueValues, long numTrueValues, long numFalseValues, Dataset<Row> frequencyValuesDS) throws JSONException {
        AtlasConsumer getTableName = new AtlasConsumer();
        String idTableName = getTableName.getIDAtlasTableACTIVE(tablename, database);
        String idColumn = getTableName.getIDAtlasColumnACTIVE(columnName, tablename, database);

        JSONObject jsonfinal = new JSONObject();
        jsonfinal.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference");

        JSONObject id = new JSONObject();
        id.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        id.put("version", 0);
        id.put("typeName", "ColumnStatistics");
        jsonfinal.put("id", id);
        
        jsonfinal.put("typeName", "ColumnStatistics");
        
        JSONObject values = new JSONObject();
        values.put("name", columnName);
        values.put("dataTypeValue", datatype);

        JSONObject tablenameEntity = new JSONObject();
        tablenameEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        tablenameEntity.put("id", idTableName);
        tablenameEntity.put("version", 0);
        tablenameEntity.put("typeName", "hive_table");
        tablenameEntity.put("state", "ACTIVE");
        values.put("tableName", tablenameEntity);

        JSONObject frequencyValues = new JSONObject();
        for (int i = 0; i < frequencyValuesDS.collectAsList().size(); i++) {
            frequencyValues.put(frequencyValuesDS.select(columnName).collectAsList().get(i).mkString(), frequencyValuesDS.select("count").collectAsList().get(i).mkString());

        }
        values.put("FrequencyValues", frequencyValues);

        JSONObject columnEntity = new JSONObject();
        columnEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        columnEntity.put("id", idColumn);
        columnEntity.put("version", 0);
        columnEntity.put("typeName", "hive_column");
        columnEntity.put("state", "ACTIVE");
        values.put("columnReference", columnEntity);

        Instant instant = Instant.now();
        values.put("createTime", instant.toString());
        values.put("comment", comment);
        values.put("owner", "adminLiD4");
        values.put("description", "Profiling attribute " + columnName + " from " + tablename + ". DB: " + database);
        values.put("qualifiedName", "st." + database + "." + tablename + "." + columnName);
        values.put("description", "Profiling attribute " + columnName + " from " + tablename + ". DB: " + database);
        values.put("numEmptyValues", (int) emptyValues);
        values.put("maxValue", max);
        values.put("minValue", min);
        values.put("maxFieldLenght", Integer.parseInt(maxFieldLength));
        values.put("minFieldLenght", Integer.parseInt(minFieldLength));
        values.put("numNullValues", (int) nullValues);
        values.put("numFalseValues", (int) numFalseValues);
        values.put("numTrueValues", (int) numTrueValues);
        values.put("PercentFillRecords", (int) percentFillRecords);
        values.put("PercentUniqueValues", (int) percentUniqueValues);
        values.put("numUniqueValues", (int) uniqueValues);
        values.put("numRecords", (int) recordCount);

        jsonfinal.put("values", values);

        JSONArray traitNames = new JSONArray();
        jsonfinal.put("traitNames", traitNames);
        JSONObject traits = new JSONObject();
        jsonfinal.put("traits", traits);

        return jsonfinal;

    }

    public JSONObject createEntityProcess(String tableName, String database, String startdate, String endDate) throws JSONException {

        AtlasConsumer restConsumer = new AtlasConsumer();
        String idTableName = restConsumer.getIDAtlasTableACTIVE(tableName, database);
        String idTableStatistics = restConsumer.getIDTableStatistics(idTableName);

        JSONObject jsonfinal = new JSONObject();
        jsonfinal.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference");
        JSONObject id = new JSONObject();
        id.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        id.put("version", 0);
        id.put("typeName", "Process");
        jsonfinal.put("id", id);

        jsonfinal.put("typeName", "ProfilerProcesses");
        JSONObject values = new JSONObject();

        values.put("startTime", startdate);
        values.put("endTime", endDate);

        values.put("userName", "rajops");
        values.put("operationType", "Profiler Quality Operation");
        values.put("clusterName", "lid4.dsi.uminho.pt");
        values.put("query", null);

        JSONArray inputs = new JSONArray();
        JSONObject inputEntity = new JSONObject();
        inputEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        inputEntity.put("id", idTableName);
        inputEntity.put("version", 0);
        inputEntity.put("typeName", "hive_table");
        inputEntity.put("state", "ACTIVE");
        inputs.add(inputEntity);
        values.put("inputs", inputs);

        JSONArray outputs = new JSONArray();
        JSONObject outputEntity = new JSONObject();
        outputEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        outputEntity.put("id", idTableStatistics);
        outputEntity.put("version", 0);
        outputEntity.put("typeName", "TableStatistics");
        outputEntity.put("state", "ACTIVE");
        outputs.add(outputEntity);
        values.put("outputs", outputs);

        values.put("qualifiedName", "prc." + database + "." + tableName);
        values.put("name", "Profiler Table " + tableName);
        values.put("description", "Profiler Quality Process ");
        values.put("owner", "EDM_RANDD");

        jsonfinal.put("values", values);
        org.json.simple.JSONArray traitNames = new org.json.simple.JSONArray();
        jsonfinal.put("traitNames", traitNames);
        org.json.simple.JSONObject traits = new org.json.simple.JSONObject();
        jsonfinal.put("traits", traits);
        return jsonfinal;
    }

    public JSONObject createEntityProcessColumnProfiler(String tableName, String database, String startdate, String endDate, String[] outputcols) throws JSONException {

        AtlasConsumer restConsumer = new AtlasConsumer();
        String idTableName = restConsumer.getIDAtlasTableACTIVE(tableName, database);

        JSONObject jsonfinal = new JSONObject();
        jsonfinal.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference");
        JSONObject id = new JSONObject();
        id.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        id.put("version", 0);
        id.put("typeName", "Process");
        jsonfinal.put("id", id);

        jsonfinal.put("typeName", "ProfilerProcesses");
        JSONObject values = new JSONObject();

        values.put("startTime", startdate);
        values.put("endTime", endDate);

        values.put("userName", "rajops");
        values.put("operationType", "Column Profiler Quality Task");
        values.put("clusterName", "lid4.dsi.uminho.pt");
        values.put("query", null);

        JSONArray inputs = new JSONArray();
        JSONObject inputEntity = new JSONObject();
        inputEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        inputEntity.put("id", idTableName);
        inputEntity.put("version", 0);
        inputEntity.put("typeName", "hive_table");
        inputEntity.put("state", "ACTIVE");
        inputs.add(inputEntity);
        values.put("inputs", inputs);

        JSONArray outputs = new JSONArray();
        for (String columnName : outputcols) {
            String idColumnStatistics = restConsumer.getIDColumnStatistics(idTableName, columnName);
            JSONObject outputEntity = new JSONObject();
            outputEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            outputEntity.put("id", idColumnStatistics);
            outputEntity.put("version", 0);
            outputEntity.put("typeName", "ColumnStatistics");
            outputEntity.put("state", "ACTIVE");
            outputs.add(outputEntity);
        }

        values.put("outputs", outputs);
        values.put("qualifiedName", "prc." + tableName);
        values.put("name", "Profiler Column From Table " + tableName);
        values.put("description", "Profiler Column  Process");
        values.put("owner", "EDM_RANDD");

        jsonfinal.put("values", values);
        org.json.simple.JSONArray traitNames = new org.json.simple.JSONArray();
        jsonfinal.put("traitNames", traitNames);
        org.json.simple.JSONObject traits = new org.json.simple.JSONObject();
        jsonfinal.put("traits", traits);
        return jsonfinal;
    }

    public JSONObject createEntityDatasetProfiler(String database, String tablename, int numCategoricalColumns, int numDateColumns, int numObservations,
            int numVariable, int numNumericalColumns, int numOtherTypesColumns) throws JSONException {

        AtlasConsumer restConsumer = new AtlasConsumer();
        String idTableName = restConsumer.getIDAtlasTableACTIVE(tablename, database);
        String idDB = restConsumer.getDBID(idTableName);
        ArrayList<String> arrayColumnStats = restConsumer.getColumnStatsID(idTableName);
        JSONObject jsonfinal = new JSONObject();
        jsonfinal.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference");

        JSONObject id = new JSONObject();
        id.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        id.put("version", 0);
        id.put("typeName", "TableStatistics");
        jsonfinal.put("id", id);

        jsonfinal.put("typeName", "TableStatistics");

        JSONObject values = new JSONObject();
        values.put("name", tablename);
        values.put("owner", "rajops");
        values.put("description", "Profiling table " + tablename + " from " + database + " database");
        values.put("qualifiedName", "st." + database + "." + tablename);

        JSONObject tableEntity = new JSONObject();
        tableEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        tableEntity.put("id", idTableName);
        tableEntity.put("version", 0);
        tableEntity.put("typeName", "hive_table");
        tableEntity.put("state", "ACTIVE");
        values.put("table", tableEntity);
        values.put("dataSetSize", 2323);

        JSONObject dbObject = new JSONObject();
        dbObject.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        dbObject.put("id", idDB);
        dbObject.put("version", 0);
        dbObject.put("typeName", "hive_db");
        dbObject.put("state", "ACTIVE");
        values.put("db", dbObject);

        JSONArray columnsStats = new JSONArray();
        for (String columnSts : arrayColumnStats) {
            JSONObject columnStatsEntity = new JSONObject();
            columnStatsEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            columnStatsEntity.put("id", columnSts);
            columnStatsEntity.put("version", 0);
            columnStatsEntity.put("typeName", "ColumnStatistics");
            columnStatsEntity.put("state", "ACTIVE");
            columnsStats.add(columnStatsEntity);
        }
        values.put("columnStatistics", columnsStats); //array

        values.put("numCategoricalColumns", numCategoricalColumns);
        values.put("numDateColumns", numDateColumns);
        values.put("numObservations", numObservations);
        values.put("numVariables", numVariable);
        values.put("numNumericalColumns", numNumericalColumns);
        values.put("numOtherTypesColumns", numOtherTypesColumns);

        jsonfinal.put("values", values);
        org.json.simple.JSONArray traitNames = new org.json.simple.JSONArray();
        jsonfinal.put("traitNames", traitNames);
        org.json.simple.JSONObject traits = new org.json.simple.JSONObject();
        jsonfinal.put("traits", traits);

        return jsonfinal;

    }

    public static void main(String[] args) throws IOException {
        JSONObject jsonfinal = new JSONObject();
        jsonfinal.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference");

        JSONObject id = new JSONObject();
        id.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        id.put("version", 0);
        id.put("typeName", "TableStatistics");
        jsonfinal.put("id", id);

        jsonfinal.put("typeName", "TableStatistics");

        JSONObject values = new JSONObject();
        values.put("name", "forestfires");
        values.put("owner", "rajops");
        values.put("description", "Profiling attribute + attribute from + table");
        values.put("qualifiedName", "st.profiler.forestfires");

        JSONObject tablename = new JSONObject();
        tablename.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        tablename.put("id", "d483386c-b26f-4529-aeef-54832c437e50");
        tablename.put("version", 0);
        tablename.put("typeName", "hive_table");
        tablename.put("state", "ACTIVE");
        values.put("table", tablename);

        JSONObject dbObject = new JSONObject();
        dbObject.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        dbObject.put("id", "4f678ec3-e348-48ae-b4a9-668368ef314b");
        dbObject.put("version", 0);
        dbObject.put("typeName", "hive_db");
        dbObject.put("state", "ACTIVE");
        values.put("db", dbObject);

        JSONArray columnsStats = new JSONArray();
        JSONObject columnStatsEntity = new JSONObject();
        columnStatsEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        columnStatsEntity.put("id", "afd18320-782a-400c-a009-9f7773f0e627");
        columnStatsEntity.put("version", 0);
        columnStatsEntity.put("typeName", "ColumnStatistics");
        columnStatsEntity.put("state", "ACTIVE");
        columnsStats.add(columnStatsEntity);

        JSONObject columnStatsEntity1 = new JSONObject();
        columnStatsEntity1.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        columnStatsEntity1.put("id", "afd18320-782a-400c-a009-9f7773f0e627");
        columnStatsEntity1.put("version", 0);
        columnStatsEntity1.put("typeName", "ColumnStatistics");
        columnStatsEntity1.put("state", "ACTIVE");
        columnsStats.add(columnStatsEntity1);

        values.put("columnStatistics", columnsStats);

        values.put("numCategoricalColumns", 2);
        values.put("numDateColumns", 3);
        values.put("numObservations", 2);
        values.put("numVariables", 2);
        values.put("numNumericalColumns", 2);
        values.put("numOtherTypesColumns", 3);

        jsonfinal.put("values", values);
        org.json.simple.JSONArray traitNames = new org.json.simple.JSONArray();
        jsonfinal.put("traitNames", traitNames);
        org.json.simple.JSONObject traits = new org.json.simple.JSONObject();
        jsonfinal.put("traits", traits);

// try-with-resources statement based on post comment below :)
        try (FileWriter file = new FileWriter("entity.json")) {
            file.write(jsonfinal.toJSONString());
            System.out.println("Successfully Copied JSON Object to File...");
            System.out.println("\nJSON Object: " + jsonfinal);
        }

    }

}
