/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package basicProfiler;

import Controller.JsonControler;
import advancedProfiler.FrequencyAnalysis;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import org.json.JSONObject;
import scala.Tuple2;


/**
 *
 * @author Utilizador
 */
public final class ColumnProfiler implements Serializable {

    private String columnName;
    private String datatypeCol;
    private String minValue;
    private String maxValue;
    private long recordCount;
    private long uniqueValues;
    private long emptyValues;
    private long nullValues;
    private String maxFieldLength;
    private String minFieldLength;
    private long percentFillRecords;
    private long percentUniqueValues;
    private long numTrueValues;
    private long numFalseValues;
    private JSONObject jsonColumnProfiler;

    public ColumnProfiler(String columnName, long recordCount, long uniqueValues, long emptyStringValues, long nullValues, String minValue, String maxValue, String maxFieldLength, String minFieldLength, String datatype, long numTrueValues, long numFalseValues, JSONObject jsonEntity) {
        this.columnName = columnName;
        this.recordCount = recordCount;
        this.uniqueValues = uniqueValues;
        this.emptyValues = emptyStringValues;
        this.nullValues = nullValues;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.maxFieldLength = maxFieldLength;
        this.percentFillRecords = calculatedPercentFill(nullValues, emptyStringValues, recordCount);
        this.percentUniqueValues = calculatePercentUnique(uniqueValues, recordCount);
        this.datatypeCol = datatype;
        this.minFieldLength = minFieldLength;
        this.numFalseValues = numFalseValues;
        this.numTrueValues = numTrueValues;
        this.jsonColumnProfiler = jsonEntity;
    }

    public ColumnProfiler() {
    }

    public JSONObject getJsonColumnProfiler() {
        return jsonColumnProfiler;
    }

    public void setJsonColumnProfiler(JSONObject jsonColumnProfiler) {
        this.jsonColumnProfiler = jsonColumnProfiler;
    }

    public long getNumTrueValues() {
        return numTrueValues;
    }

    public void setNumTrueValues(long numTrueValues) {
        this.numTrueValues = numTrueValues;
    }

    public long getNumFalseValues() {
        return numFalseValues;
    }

    public void setNumFalseValues(long numFalseValues) {
        this.numFalseValues = numFalseValues;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDatatypeCol() {
        return datatypeCol;
    }

    public void setDatatypeCol(String datatypeCol) {
        this.datatypeCol = datatypeCol;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public long getUniqueValues() {
        return uniqueValues;
    }

    public void setUniqueValues(long uniqueValues) {
        this.uniqueValues = uniqueValues;
    }

    public long getEmptyValues() {
        return emptyValues;
    }

    public void setEmptyValues(long emptyValues) {
        this.emptyValues = emptyValues;
    }

    public long getNullValues() {
        return nullValues;
    }

    public void setNullValues(long nullValues) {
        this.nullValues = nullValues;
    }

    public String getMinValue() {
        return minValue;
    }

    public void setMinValue(String minValue) {
        this.minValue = minValue;
    }

    public String getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(String maxValue) {
        this.maxValue = maxValue;
    }

    public String getMaxFieldLength() {
        return maxFieldLength;
    }

    public void setMaxFieldLength(String maxFieldLength) {
        this.maxFieldLength = maxFieldLength;
    }

    public long getPercentFillRecords() {
        return percentFillRecords;
    }

    public void setPercentFillRecords(long percentFillRecords) {
        this.percentFillRecords = percentFillRecords;
    }

    public long getPercentUniqueValues() {
        return percentUniqueValues;
    }

    public void setPercentUniqueValues(long percentUniqueValues) {
        this.percentUniqueValues = percentUniqueValues;
    }

    public String getMinFieldLength() {
        return minFieldLength;
    }

    public void setMinFieldLength(String minFieldLength) {
        this.minFieldLength = minFieldLength;
    }

    public long calculatePercentUnique(long uniqueValues, long totalRecords) {
        long percentUnique = (long) ((uniqueValues * 100) / totalRecords);
        return (percentUnique);
    }

    public long calculatedPercentFill(long nullvalues, long emptyStringValues, long totalRecords) {

        double filledRecords = totalRecords - nullValues - emptyStringValues;
        long percentFill = (long) ((filledRecords * 100) / totalRecords);
        return (percentFill);
    }

    public long calculatedPercentNumeric(long numericValues, long totalRecords) {

        long percentFillNumeric = (long) ((numericValues * 100) / totalRecords);
        return (percentFillNumeric);
    }

    /*
     */
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

    /*
     * @param 
     */
    public ColumnProfiler basicProfiler(Dataset<Row> dataSet, String columnName, String tableName, String database) {
        /// Data Frame describe function 
        Dataset<Row> col = dataSet.select(col(columnName));
        String datatypes = dataTypeColumn(dataSet.dtypes(), columnName);

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
            Dataset< Row> describeData = dataSet.describe(columnName);
            List<Row> obj = describeData.select(columnName).collectAsList();
            minValue = obj.get(3).mkString();
            maxValue = obj.get(4).mkString();
        }
        // If the type is TimesTamp;
        if (datatypes.equalsIgnoreCase("TimestampType") || datatypes.equalsIgnoreCase("BooleanType")) {
            if (datatypes.equalsIgnoreCase("BooleanType")) {
                Dataset<Row> trueValuesDataSet = dataSet.filter(col(columnName).equalTo(true));
                Dataset<Row> falseValuesDataSet = dataSet.filter(col(columnName).equalTo(false));
                truevaluecount = trueValuesDataSet.count();
                falsevaluecount = falseValuesDataSet.count();
            } else {
                List<Row> listmaxDate = col.withColumn("maxDate", col(columnName)).agg(org.apache.spark.sql.functions.max(col(columnName))).collectAsList();
                Timestamp maxtime = Timestamp.valueOf(listmaxDate.get(0).mkString());
                maxValue = maxtime.toString();
                List<Row> listminDate = col.withColumn("minDate", col(columnName)).agg(org.apache.spark.sql.functions.min(col(columnName))).collectAsList();
                Timestamp mintime = Timestamp.valueOf(listminDate.get(0).mkString());
                minValue = mintime.toString();
            }
        } else {
            List<Row> listmaxVal = col.agg(org.apache.spark.sql.functions.max(col(columnName))).collectAsList();
            maxFieldLength = String.valueOf(listmaxVal.get(0).mkString().length());
            List<Row> listminVal = col.withColumn("fieldleng", col(columnName)).agg(org.apache.spark.sql.functions.min(col(columnName))).collectAsList();
            minFieldLength = String.valueOf(listminVal.get(0).mkString().length());
            emptyvalues = col.withColumn("isEmpty", col(columnName).isNaN()).filter(col("isEmpty").equalTo(true)).count();

        }
        long recordCounts = col.count();
        long uniqueValue = col.distinct().count();
        long nullvalues = col.withColumn("isNull", col(columnName).isNull()).filter(col("isNull").equalTo(true)).count();
        long percentFillRecords = calculatedPercentFill(nullvalues, emptyvalues, recordCounts);
        long percentUniqueValues = calculatePercentUnique(uniqueValue, recordCounts);

        String comment = "Profiler Analysis";
        //FrequencyAnalysis - (Package advancedProfiler.FrequencyAnalysis)
        FrequencyAnalysis freqAnalysis = new FrequencyAnalysis();
        Dataset<Row> frequencyValues = freqAnalysis.frequencyValuesAnalysis(dataSet, columnName);
        JsonControler jsonAtlas = new JsonControler();
        JSONObject jsonEntity = jsonAtlas.createEntityColumnProfiler(columnName, datatypes, database, tableName, comment, minValue, maxValue, recordCounts, uniqueValue, emptyvalues, nullvalues, maxFieldLength, minFieldLength, percentFillRecords, percentUniqueValues, truevaluecount, falsevaluecount, frequencyValues);
        ColumnProfiler columnnew = new ColumnProfiler(columnName, recordCounts, uniqueValue, emptyvalues, nullvalues, minValue, maxValue, maxFieldLength, minFieldLength, datatypes, truevaluecount, falsevaluecount, jsonEntity);
        return (columnnew);
    }

    
    @Override
    public String toString() {
        return "ColumnProfiler{" + "columnName=" + columnName + ", datatypeCol=" + datatypeCol + ", min=" + minValue + ", max=" + maxValue + ", recordCount=" + recordCount + ", uniqueValues=" + uniqueValues + ", emptyStringValues=" + emptyValues + ", nullValues=" + nullValues + ", maxFieldLength=" + maxFieldLength + ", minFieldLength=" + minFieldLength + ", percentFillRecords=" + percentFillRecords + ", percentUniqueValues=" + percentUniqueValues + '}';
    }

}
