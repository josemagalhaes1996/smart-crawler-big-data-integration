/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package advancedProfiler;

import basicProfiler.ColumnProfiler;
import com.hortonworks.hwc.Connections;
import basicProfiler.DataSetProfiler;
import basicProfiler.Profiler;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

/**
 *
 * @author Utilizador
 */
public class CorrelationAnalysis implements Serializable {

    private String attribute;
    private String correlationValue;
    private String correlationAttribute;

    public CorrelationAnalysis(String attribute, String correlationValue, String correlationAttribute) {
        this.attribute = attribute;
        this.correlationValue = correlationValue;
        this.correlationAttribute = correlationAttribute;
    }

    public CorrelationAnalysis() {
    }

    public String getCorrelationAttribute() {
        return correlationAttribute;
    }

    public void setCorrelationAttribute(String correlationAttribute) {
        this.correlationAttribute = correlationAttribute;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public String getCorrelationValue() {
        return correlationValue;
    }

    public void setCorrelationValue(String correlationValue) {
        this.correlationValue = correlationValue;
    }

    public static void main(String args[]) {
        Connections conn = new Connections();
        Profiler prof = new Profiler("foodmart", "store", conn);
        runCorrelations(conn, prof.getDataSet());
    }

    public static void runCorrelations(Connections conn, Dataset<Row> dataset) {
        String[] columnnames = dataset.columns();
        ArrayList<CorrelationAnalysis> correlationList = new ArrayList<>();
        for (String columnA : columnnames) {
            for (String columnB : columnnames) {
                if (columnA.equals(columnB)) {
                } else {
                    ColumnProfiler dtype = new ColumnProfiler();
                    if (dtype.dataTypeColumn(dataset.dtypes(), columnA).equalsIgnoreCase("StringType")
                            || dtype.dataTypeColumn(dataset.dtypes(), columnA).equalsIgnoreCase("TimestampType")
                            || dtype.dataTypeColumn(dataset.dtypes(), columnA).equalsIgnoreCase("BooleanType")
                            || dtype.dataTypeColumn(dataset.dtypes(), columnB).equalsIgnoreCase("StringType")
                            || dtype.dataTypeColumn(dataset.dtypes(), columnB).equalsIgnoreCase("TimestampType")
                            || dtype.dataTypeColumn(dataset.dtypes(), columnB).equalsIgnoreCase("BooleanType")) {

                    } else {
                        double correlationValue = dataset.stat().corr(columnA, columnB);
                        CorrelationAnalysis correlationobject = new CorrelationAnalysis(columnA, String.valueOf(correlationValue), columnB);
                        correlationList.add(correlationobject);
                    }
                }
            }

        }
        Encoder<CorrelationAnalysis> correlationEncoder = Encoders.bean(CorrelationAnalysis.class);
        Dataset<CorrelationAnalysis> dataSetCorrelation = conn.getSession().createDataset(Collections.synchronizedList(correlationList), correlationEncoder);
        dataSetCorrelation.show();
    }
}
