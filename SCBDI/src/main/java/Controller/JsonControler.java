/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controller;

/**
 *
 * @author Utilizador
 */
import AtlasClient.AtlasConsumer;
import com.hortonworks.hwc.Connections;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonControler {

    public JsonControler() {
    }
    /*
     This function has the purpose of creating json to create an entity in the Atlas.
     It is invoked in the ColumnProfiler Class (package basicProfiler)
     All parameters are related with statistics of one column. 
            
     */

    public void updateNumAudits(String tableName, String dbName) throws JSONException, IOException {

        AtlasConsumer consumer = new AtlasConsumer();
        JSONObject jsonTable = consumer.getGuidHiveTable(tableName, "hive_table");
        JSONObject jsonDb = consumer.getGuidHiveTable(dbName, "hive_db");
        String guidDataBase = jsonDb.getJSONArray("results").getJSONObject(0).getJSONObject("$id$").getString("id");
        String guidTable = null;

        for (int i = 0; i < jsonDb.getJSONArray("results").length(); i++) {

            if (jsonTable.getJSONArray("results").getJSONObject(i).getJSONObject("db").getString("id").equals(guidDataBase)) {
                guidTable = jsonTable.getJSONArray("results").getJSONObject(i).getJSONObject("$id$").getString("id");

            }

        }

        System.out.println("O GUID da tabela é " + guidTable);
        int jsonAudtis = consumer.getAudtisTable(guidTable).length();

        System.out.println("Audtis é  " + jsonAudtis);
        consumer.updateAuditsTable(jsonAudtis, guidTable);
    }

    public JSONObject createEntityColumnProfiler(String columnName, String datatype, String database, String tablename,
            String comment, String min, String max, long recordCount, long uniqueValues, long emptyValues, long nullValues,
            String maxFieldLength, String minFieldLength, long percentFillRecords, long percentUniqueValues, long numTrueValues, long numFalseValues, Dataset<Row> frequencyValuesDS) {

        AtlasConsumer getTableName = new AtlasConsumer();
        JSONObject jsonfinal = null;
        try {
            String idTableName = getTableName.getIDAtlasTableACTIVE(tablename, database);

            String idColumn = getTableName.getIDAtlasColumnACTIVE(columnName, tablename, database);

            jsonfinal = new JSONObject();
            jsonfinal.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference");

            JSONObject id = new JSONObject();
            id.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            id.put("id", "-1466683608564093000");
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
        } catch (Exception e) {
            e.getMessage();
        }
        return jsonfinal;
    }

    public JSONObject createEntityJob(String jobName, String sparkUser) {
        JSONObject jsonfinal = null;
        try {

            jsonfinal = new JSONObject();
            jsonfinal.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference");

            JSONObject id = new JSONObject();
            id.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            id.put("id", "-1466683608564093000");
            id.put("version", 0);
            id.put("typeName", "Job");
            jsonfinal.put("id", id);
            jsonfinal.put("typeName", "Job");

            JSONObject values = new JSONObject();
            values.put("name", jobName);
            values.put("sparkUser", sparkUser);

            values.put("qualifiedName", jobName);

            jsonfinal.put("values", values);
            JSONArray traitNames = new JSONArray();
            jsonfinal.put("traitNames", traitNames);
            JSONObject traits = new JSONObject();
            jsonfinal.put("traits", traits);
        } catch (Exception e) {
            e.getMessage();
        }
        return jsonfinal;
    }

    public JSONObject createEntityInterStatistics(String tableBDW, String tableNewSource, String columnBDW,
            String columnNS, double intersectionResultContent, double similarityHeader, double threshold, String database) {
        JSONObject jsonfinal = null;
        AtlasConsumer atlas = new AtlasConsumer();
        try {

            jsonfinal = new JSONObject();
            jsonfinal.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference");

            JSONObject id = new JSONObject();
            id.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            id.put("id", "-146668360");
            id.put("version", 0);
            id.put("typeName", "InterStatistics");
            jsonfinal.put("id", id);
            jsonfinal.put("typeName", "InterStatistics");

            JSONObject values = new JSONObject();
            values.put("name", "InterStatistics between " + columnBDW + " and " + columnNS + " columns . |Tables: " + tableBDW + " and " + tableNewSource);
            values.put("thresholdFilter", threshold);
            values.put("contentSimilarity", intersectionResultContent);
            values.put("headerSimilarity", similarityHeader);

            System.out.println("chegou aqui" );

            
            String idColumnBDW = atlas.getIDAtlasColumnACTIVE(columnBDW, tableBDW, database);
            JSONObject columnMain = new JSONObject();
            columnMain.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            columnMain.put("id", idColumnBDW);
            columnMain.put("version", 0);
            columnMain.put("typeName", "hive_column");
            columnMain.put("state", "ACTIVE");
            values.put("columnMain", columnMain);
            
            System.out.println("chegou aqui" + idColumnBDW);

            String idColumnNS = atlas.getIDAtlasColumnACTIVE(columnNS, tableNewSource, database);
            JSONObject columnNSJson = new JSONObject();
            columnNSJson.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            columnNSJson.put("id", idColumnNS);
            columnNSJson.put("version", 0);
            columnNSJson.put("typeName", "hive_column");
            columnNSJson.put("state", "ACTIVE");
            values.put("columnToCompare", columnNSJson);
            values.put("qualifiedName", "interStatistics." +columnBDW+"."+ columnNS+"."+ tableBDW + "." + tableNewSource + "." + database);

            
          System.out.println("ultimo passo antes da tabela");

            
            JSONArray inputs = new JSONArray();
            String idTableBDW = atlas.getIDAtlasTableACTIVE(tableBDW, database);
            String idTableNS = atlas.getIDAtlasTableACTIVE(tableNewSource, database);

            JSONObject tablesBDW = new JSONObject();
            tablesBDW.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            tablesBDW.put("id", idTableBDW);
            tablesBDW.put("version", 0);
            tablesBDW.put("typeName", "hive_table");
            tablesBDW.put("state", "ACTIVE");
            inputs.put(tablesBDW);
            
            JSONObject NSTable = new JSONObject();
            NSTable.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            NSTable.put("id", idTableNS);
            NSTable.put("version", 0);
            NSTable.put("typeName", "hive_table");
            NSTable.put("state", "ACTIVE");
            inputs.put(NSTable);


            values.put("tables",inputs);

            jsonfinal.put("values", values);
            JSONArray traitNames = new JSONArray();
            jsonfinal.put("traitNames", traitNames);
            JSONObject traits = new JSONObject();
            jsonfinal.put("traits", traits);
        } catch (Exception e) {
            e.getMessage();
        }
        return jsonfinal;
    }
    /*
     This function has the purpose of creating json to create a Process Entity in the Atlas.
     It is invoked in  Profiler Class (package basicProfiler)
     TableName - TableName 
     Database - database related to Table 
     StartDate - Json read as String but the format is Date . Date when the process started
     EndDate - Date when the process was finished; 
            
     */

    public JSONObject createEntityProcess(String tableName, String database, String startdate, String endDate, String qualifiedName) {
        AtlasConsumer restConsumer = new AtlasConsumer();
        JSONObject jsonfinal = null;
        try {
            String idTableName = restConsumer.getIDAtlasTableACTIVE(tableName, database);
            String idTableStatistics = restConsumer.getIDTableStatistics(idTableName);

            jsonfinal = new JSONObject();
            jsonfinal.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference");
            JSONObject id = new JSONObject();
            id.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            id.put("id", "-1466683608564093000");
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
            values.put("query", "");

            JSONArray inputs = new JSONArray();
            JSONObject inputEntity = new JSONObject();
            inputEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            inputEntity.put("id", idTableName);
            inputEntity.put("version", 0);
            inputEntity.put("typeName", "hive_table");
            inputEntity.put("state", "ACTIVE");
            inputs.put(inputEntity);

            values.put("inputs", inputs);

            JSONArray outputs = new JSONArray();
            JSONObject outputEntity = new JSONObject();
            outputEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            outputEntity.put("id", idTableStatistics);
            outputEntity.put("version", 0);
            outputEntity.put("typeName", "TableStatistics");
            outputEntity.put("state", "ACTIVE");
            outputs.put(outputEntity);
            values.put("outputs", outputs);

            String idJob = restConsumer.getJob(qualifiedName);
            System.out.println("\n" + " Encontrou o id e é este" + idJob);
            JSONObject job = new JSONObject();
            job.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            job.put("id", idJob);
            job.put("version", 0);
            job.put("typeName", "Job");
            job.put("state", "ACTIVE");
            values.put("job", job);

            values.put("qualifiedName", "prc." + database + "." + tableName);
            values.put("name", "Profiler Table " + tableName);
            values.put("description", "Profiler Quality Process ");
            values.put("owner", "EDM_RANDD");

            jsonfinal.put("values", values);
            JSONArray traitNames = new JSONArray();
            jsonfinal.put("traitNames", traitNames);
            JSONObject traits = new JSONObject();
            jsonfinal.put("traits", traits);

        } catch (Exception e) {
            e.getMessage();
        }
        return jsonfinal;
    }

    /*
     This function has the purpose of creating json to create a Process Entity in the Atlas ( Profiler Column  Process).
     It is invoked in  Profiler Class (package basicProfiler)
     TableName - TableName 
     Database - database related to Table 
     StartDate - Json read as String but the format is Date . Date when the process started
     EndDate - Date when the process was finished; 
     OutputCols - All columns are related with the profiler processes ( 
            
     */
    public JSONObject createEntityProcessColumnProfiler(String tableName, String database, String startdate, String endDate, String[] outputcols) {
        try {
            AtlasConsumer restConsumer = new AtlasConsumer();
            String idTableName = restConsumer.getIDAtlasTableACTIVE(tableName, database);

            JSONObject jsonfinal = new JSONObject();
            jsonfinal.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference");
            JSONObject id = new JSONObject();
            id.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            id.put("id", "-1466683608564093000");
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
            values.put("query", "");

            JSONArray inputs = new JSONArray();
            JSONObject inputEntity = new JSONObject();
            inputEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
            inputEntity.put("id", idTableName);
            inputEntity.put("version", 0);
            inputEntity.put("typeName", "hive_table");
            inputEntity.put("state", "ACTIVE");
            inputs.put(inputEntity);
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
                outputs.put(outputEntity);
            }

            values.put("outputs", outputs);
            values.put("qualifiedName", "prc." + tableName);
            values.put("name", "Profiler Column From Table " + tableName);
            values.put("description", "Profiler Column  Process");
            values.put("owner", "EDM_RANDD");

            jsonfinal.put("values", values);
            JSONArray traitNames = new JSONArray();
            jsonfinal.put("traitNames", traitNames);
            JSONObject traits = new JSONObject();
            jsonfinal.put("traits", traits);
            return jsonfinal;

        } catch (Exception e) {
            e.getMessage();
        }
        return null;
    }

    /*
     This function has the purpose of creating json to create a TableStatistics Entity in the Atlas ( Profiler Table  Process).
     It is invoked in  Profiler Class (package basicProfiler)
     TableName - TableName 
     Database - database related to Table 
     StartDate - Json read as String but the format is Date . Date when the process started
     EndDate - Date when the process was finished; 
     OutputCols - All columns are related with the profiler processes (             
     */
    public JSONObject createEntityTableProfiler(String database, String tablename, int numCategoricalColumns, int numDateColumns, int numObservations,
            int numVariable, int numNumericalColumns, int numOtherTypesColumns, int dataSetSize) throws JSONException {

        JSONObject jsonfinal = null;
        AtlasConsumer restConsumer = new AtlasConsumer();
        String idTableName = restConsumer.getIDAtlasTableACTIVE(tablename, database);
        String idDB = restConsumer.getDBID(idTableName);
        ArrayList<String> arrayColumnStats = restConsumer.getColumnStatsID(idTableName);
        jsonfinal = new JSONObject();
        jsonfinal.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference");

        JSONObject id = new JSONObject();
        id.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        id.put("id", "-2521");
        id.put("version", 0);
        id.put("typeName", "TableStatistics");
        jsonfinal.put("id", id);

        jsonfinal.put("typeName", "TableStatistics");

        JSONObject values = new JSONObject();
        values.put("name", tablename);
        values.put("owner", "admin");
        values.put("description", "Profiling table " + tablename + " from " + database + " database");
        values.put("qualifiedName", "st." + database + "." + tablename);

        JSONObject tableEntity = new JSONObject();
        tableEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        tableEntity.put("id", idTableName);
        tableEntity.put("version", 0);
        tableEntity.put("typeName", "hive_table");
        tableEntity.put("state", "ACTIVE");
        values.put("table", tableEntity);
        values.put("dataSetSize", dataSetSize);

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
            columnsStats.put(columnStatsEntity);
        }
        values.put("columnStatistics", columnsStats); //array

        values.put("numCategoricalColumns", numCategoricalColumns);
        values.put("numDateColumns", numDateColumns);
        values.put("numObservations", numObservations);
        values.put("numVariables", numVariable);
        values.put("numNumericalColumns", numNumericalColumns);
        values.put("numOtherTypesColumns", numOtherTypesColumns);

        jsonfinal.put("values", values);

        JSONArray traitNames = new JSONArray();
        jsonfinal.put("traitNames", traitNames);
        JSONObject traits = new JSONObject();
        jsonfinal.put("traits", traits);

        return jsonfinal;
    }

    public JSONObject createEntityIntraStatistics(String tablename, String database, String columnMain, String columnToCompare, double correlationValue) throws JSONException {

        JSONObject jsonfinal = null;
        AtlasConsumer restConsumer = new AtlasConsumer();
        String idTableName = restConsumer.getIDAtlasTableACTIVE(tablename, database);
        System.out.println("passou aqui id tabe");
        String idColumnMain = restConsumer.getIDAtlasColumnACTIVE(columnMain, tablename, database);
        System.out.println("id columnmain");
        String idColumnToCompare = restConsumer.getIDAtlasColumnACTIVE(columnToCompare, tablename, database);
        System.out.println("idcolumnto Campare");
        jsonfinal = new JSONObject();
        jsonfinal.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference");

        JSONObject id = new JSONObject();
        id.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        id.put("id", "-2521");
        id.put("version", 0);
        id.put("typeName", "IntraStatistics");
        jsonfinal.put("id", id);

        jsonfinal.put("typeName", "IntraStatistics");

        JSONObject values = new JSONObject();
        values.put("name", columnMain + "****" + columnToCompare + "-  " + correlationValue);
        values.put("owner", "admin");
        values.put("description", "Correlation Analysis Between " + columnMain + " and " + columnToCompare);
        values.put("qualifiedName", "instrast." + database + "." + tablename + "." + columnMain + "." + columnToCompare);

        JSONObject columnMainJ = new JSONObject();
        columnMainJ.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        columnMainJ.put("id", idColumnMain);
        columnMainJ.put("version", 0);
        columnMainJ.put("typeName", "hive_column");
        columnMainJ.put("state", "ACTIVE");
        values.put("columnMain", columnMainJ);

        JSONObject columnToCompareJ = new JSONObject();
        columnToCompareJ.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        columnToCompareJ.put("id", idColumnToCompare);
        columnToCompareJ.put("version", 0);
        columnToCompareJ.put("typeName", "hive_column");
        columnToCompareJ.put("state", "ACTIVE");
        values.put("columnToCompare", columnToCompareJ);

        JSONObject tableEntity = new JSONObject();
        tableEntity.put("jsonClass", "org.apache.atlas.typesystem.json.InstanceSerialization$_Id");
        tableEntity.put("id", idTableName);
        tableEntity.put("version", 0);
        tableEntity.put("typeName", "hive_table");
        tableEntity.put("state", "ACTIVE");
        values.put("table", tableEntity);

        values.put("correlationValue", correlationValue); //array

        JSONArray similarityObjects = new JSONArray();
        values.put("similarityObjects", similarityObjects); //array

        jsonfinal.put("values", values);

        JSONArray traitNames = new JSONArray();
        jsonfinal.put("traitNames", traitNames);
        JSONObject traits = new JSONObject();
        jsonfinal.put("traits", traits);

        return jsonfinal;
    }

}
