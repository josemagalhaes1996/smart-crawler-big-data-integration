/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package advancedProfiler;

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

    public Dataset<Row> frequencyValuesAnalysis(Dataset<Row> dataSet, String attribute) {
        return dataSet.groupBy(col(attribute)).agg(size(collect_list(attribute))
                .as("count")).select(col(attribute), col("count")).orderBy(col("count").desc()).limit(10);
    }

    public Dataset<Row> frequencyValuesAnalysisWOLim(Dataset<Row> dataSet, String attribute) {
        return dataSet.groupBy(col(attribute)).agg(size(collect_list(attribute))
                .as("count")).select(col(attribute), col("count")).orderBy(col("count").desc());
    }

}
