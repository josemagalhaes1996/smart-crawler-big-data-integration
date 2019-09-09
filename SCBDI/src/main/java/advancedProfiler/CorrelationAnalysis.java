/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package advancedProfiler;

import AtlasClient.AtlasConsumer;
import Controller.JsonControler;
import basicProfiler.ColumnProfiler;
import com.hortonworks.hwc.Connections;
import basicProfiler.Profiler;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONException;
import org.json.JSONObject;

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

    public static void main(String args[]) throws JSONException, Exception {
        Connections conn = new Connections();
        Profiler prof = new Profiler("tpcds", "store_sales", conn);

        List<String> out = new ArrayList<>();
        runCorrelations(prof.getTable(), prof.getDatabase(), conn, prof.getDataSet(), prof.getDataSet().columns(), out, 2, 0, prof.getDataSet().columns().length);

    }

    public static void runCorrelations(String table, String database, Connections conn, Dataset<Row> dataset, String[] A, List<String> out, int k, int i, int n) throws JSONException, Exception {
        // base case: if combination size is k, print it
        if (out.size() == k) {
            if (out.get(0) == out.get(1)) {
            } else {
                System.out.println("Process to :" + out.get(0));
                ArrayList<CorrelationAnalysis> correlationList = new ArrayList<>();
                ColumnProfiler dtype = new ColumnProfiler();
                if (dtype.dataTypeColumn(dataset.dtypes(), out.get(0)).equalsIgnoreCase("StringType")
                        || dtype.dataTypeColumn(dataset.dtypes(), out.get(0)).equalsIgnoreCase("TimestampType")
                        || dtype.dataTypeColumn(dataset.dtypes(), out.get(0)).equalsIgnoreCase("BooleanType")
                        || dtype.dataTypeColumn(dataset.dtypes(), out.get(1)).equalsIgnoreCase("TimestampType")
                        || dtype.dataTypeColumn(dataset.dtypes(), out.get(1)).equalsIgnoreCase("StringType")
                        || dtype.dataTypeColumn(dataset.dtypes(), out.get(1)).equalsIgnoreCase("BooleanType")) {

                } else {
                    double correlationValue = dataset.stat().corr(out.get(0), out.get(1));
                    CorrelationAnalysis correlationobject = new CorrelationAnalysis(out.get(0), String.valueOf(correlationValue), out.get(1));
                    correlationList.add(correlationobject);
                    System.out.println("\t" + "AttributeA: " + out.get(0) + " --AtributeB: " + out.get(1) + "--------Value: " + correlationValue);
                    System.out.println("\n");

//                    //SEND INFORMATION TO ATLAS
                    AtlasConsumer restconsumer = new AtlasConsumer();
                    JsonControler jsoncontroler = new JsonControler();
                    JSONObject columnMain = jsoncontroler.createEntityIntraStatistics(table, database, out.get(0), out.get(1), correlationValue);
                    JSONObject comlumnToCompare = jsoncontroler.createEntityIntraStatistics(table, database, out.get(1), out.get(0), correlationValue);

                    restconsumer.createEntityAtlas(columnMain);
                    restconsumer.createEntityAtlas(comlumnToCompare);
                }
                //Profiler functions             
            }
            return;
        }
        // start from previous element in the current combination
        // till last element
        for (int j = i; j < n; j++) {
            // add current element A[j] to the solution and recurse with
            // same index j (as repeated elements are allowed in combinations)
            out.add(A[j]);
            runCorrelations(table, database, conn, dataset, A, out, k, j, n);
            // backtrack - remove current element from solution
            out.remove(out.size() - 1);
//	    code to handle duplicates - skip adjacent duplicate elements
            while (j < n - 1 && A[j] == A[j + 1]) {
                j++;
            }
        }

    }

}
