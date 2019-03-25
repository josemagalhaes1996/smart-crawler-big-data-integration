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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
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
        Profiler prof = new Profiler("tpcds", "store_sales", conn);

       
        ArrayList<String[]> array = new ArrayList<>();
        array = combinations2(prof.getDataSet().columns(), 2, 0, new String[2], array);

        System.out.println("Numero de Combinacoes : --- " + array.size());

      for(String[] pairs : array){
        System.out.println("ColunaA: --- " + pairs[0] + "---Outa Chave: " + pairs[1]);
      
      
      }
        

     
            
//            runCorrelations2(conn, prof.getDataSet());
    }

    public static void runCorrelations(Connections conn, Dataset<Row> dataset) {
        Dataset<Row> sample = dataset.sample(false, 0.1).limit(100);
        String[] columnnames = sample.columns();
        ArrayList<CorrelationAnalysis> correlationList = new ArrayList<>();
        for (String columnA : columnnames) {
            for (String columnB : columnnames) {
                if (columnA.equals(columnB)) {
                } else {
                    ColumnProfiler dtype = new ColumnProfiler();
                    if (dtype.dataTypeColumn(sample.dtypes(), columnA).equalsIgnoreCase("StringType")
                            || dtype.dataTypeColumn(sample.dtypes(), columnA).equalsIgnoreCase("TimestampType")
                            || dtype.dataTypeColumn(sample.dtypes(), columnA).equalsIgnoreCase("BooleanType")
                            || dtype.dataTypeColumn(sample.dtypes(), columnB).equalsIgnoreCase("TimestampType")
                            || dtype.dataTypeColumn(sample.dtypes(), columnB).equalsIgnoreCase("StringType")
                            || dtype.dataTypeColumn(sample.dtypes(), columnB).equalsIgnoreCase("BooleanType")) {

                    } else {
                        double correlationValue = sample.stat().corr(columnA, columnB);
                        CorrelationAnalysis correlationobject = new CorrelationAnalysis(columnA, String.valueOf(correlationValue), columnB);
                        correlationList.add(correlationobject);
                    }
                }
            }

        }
        Encoder<CorrelationAnalysis> correlationEncoder = Encoders.bean(CorrelationAnalysis.class);
        Dataset<CorrelationAnalysis> dataSetCorrelation = conn.getSession().createDataset(Collections.synchronizedList(correlationList), correlationEncoder);
        dataSetCorrelation.show(20);

    }

    public static void runCorrelations2(Connections conn, Dataset<Row> dataset) {

        String[] columnnames = dataset.columns();

        ArrayList<CorrelationAnalysis> correlationList = new ArrayList<>();
        ArrayList<String[]> array = new ArrayList<>();
        array = combinations2(dataset.columns(), 2, 0, new String[2], array);

        for (String[] columnPairs : array) {

            ColumnProfiler dtype = new ColumnProfiler();
            String dtypecolA = dtype.dataTypeColumn(dataset.dtypes(), columnPairs[0]);

            String dtypecolB = dtype.dataTypeColumn(dataset.dtypes(), columnPairs[1]);

            if (dtypecolA.equalsIgnoreCase("StringType") || dtypecolA.equalsIgnoreCase("TimestampType") || dtypecolA.equalsIgnoreCase("BooleanType")
                    || dtypecolB.equalsIgnoreCase("TimestampType") || dtypecolB.equalsIgnoreCase("StringType") || dtypecolB.equalsIgnoreCase("BooleanType")) {

            } else {
                double correlationValue = dataset.stat().corr(columnPairs[0], columnPairs[1]);
                                
                System.out.println("Column A -----" + columnPairs[0] + " -- COlumnB --- " + columnPairs[1] +  "  CorrelationVALUE:---- " + correlationValue  );

            }
        }

    }

    public static ArrayList<String[]> combinations2(String[] arr, int len, int startPosition, String[] result, ArrayList<String[]> array) {

        if (len == 0) {
//            System.out.println(Arrays.toString(result));

            return array;
        }
        for (int i = startPosition; i <= arr.length - len; i++) {
            result[result.length - len] = arr[i];

            array.add(result);
            combinations2(arr, len - 1, i + 1, result, array);

        }

        return array;
    }

}
