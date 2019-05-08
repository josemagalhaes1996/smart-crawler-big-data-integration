/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package advancedProfiler;

import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.size;
import scala.collection.mutable.HashTable;

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
