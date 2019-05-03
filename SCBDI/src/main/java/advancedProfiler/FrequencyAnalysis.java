/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package advancedProfiler;

import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import java.time.Instant;
import java.util.Hashtable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.size;

/**
 *
 * @author Utilizador
 */
public class FrequencyAnalysis {

    public FrequencyAnalysis() {

    }

//    public static void main(String args[]) {
//        Instant start = Instant.now();
//        Hashtable<String, String> hashtable = new Hashtable<>();
//        Hashtable<String, String> hashfinal = new Hashtable<>();
//        Connections conn = new Connections();
//        Profiler prof = new Profiler("storesale_st", "customer", conn); //New Source
//        Profiler prof2 = new Profiler("storesale_st", "customer_address", conn); // BDW
//        for (int i = 0; i < prof.getDataSet().select("c_current_addr_sk").collectAsList().size(); i++) {
//            hashtable.put(prof.getDataSet().select("c_current_addr_sk").collectAsList().get(i).mkString(), "");
//        }
//        System.out.println("Feito primeira conversao para hash");
//        for (int i = 0; i < prof2.getDataSet().select("ca_address_sk").collectAsList().size(); i++) {
//            if (hashtable.containsKey(prof2.getDataSet().select("ca_address_sk").collectAsList().get(i).mkString())) {
//                hashfinal.put(prof2.getDataSet().select("ca_address_sk").collectAsList().get(i).mkString(), "");
//            }
//        }
//        System.out.println("Tamanho hasfinal: " + hashfinal.size());
//        long intersection = (long) (hashfinal.size() / prof2.getDataSet().count());
//        System.out.println("Intersecção: " + intersection);
//        Instant endDate = Instant.now();
//    }
    public static void main(String args[]) {
        Connections conn = new Connections();
        Profiler prof = new Profiler("storesale_st", "store_sales", conn);
        FrequencyAnalysis freqAnalysis = new FrequencyAnalysis();
        Dataset<Row> frequencyValues = freqAnalysis.frequencyValuesAnalysisWOLim(prof.getDataSet(), "ss_customer_sk");

        Hashtable<String, Integer> hashtable = new Hashtable<>();

        
         for (int i = 0; i < frequencyValues.collectAsList().size(); i++) {
                hashtable.put(frequencyValues.select("ss_customer_sk").collectAsList().get(i).mkString(),Integer.parseInt(frequencyValues.select("count").collectAsList().get(i).mkString()));
            }
        
         frequencyValues.show();

    }

    public Dataset<Row> frequencyValuesAnalysis(Dataset<Row> dataSet, String attribute) {
        return dataSet.groupBy(col(attribute)).agg(size(collect_list(attribute))
                .as("count")).select(col(attribute), col("count")).orderBy(col("count").desc()).limit(10);
    }

    public Dataset<Row> frequencyValuesAnalysisWOLim(Dataset<Row> dataSet, String attribute) {
        return dataSet.groupBy(col(attribute)).agg(size(collect_list(attribute))
                .as("count")).select(col(attribute), col("count")).orderBy(col("count").desc());
    }

}
