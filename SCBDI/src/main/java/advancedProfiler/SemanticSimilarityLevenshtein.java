/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package advancedProfiler;

import basicProfiler.ColumnProfiler;
import com.hortonworks.hwc.Connections;
import basicProfiler.Profiler;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.levenshtein;

/**
 *
 * @author Utilizador
 */
public class SemanticSimilarityLevenshtein implements Serializable {

    private String attributeA;
    private String levenshteinDistance;
    private String attributeB;

    public SemanticSimilarityLevenshtein(String attribute, String similarityValue, String attributebdw) {
        this.attributeA = attribute;
        this.levenshteinDistance = similarityValue;
        this.attributeB = attributebdw;
    }

    public SemanticSimilarityLevenshtein() {
    }

    public String getAttributeA() {
        return attributeA;
    }

    public void setAttributeA(String attributeA) {
        this.attributeA = attributeA;
    }

    public String getLevenshteinDistance() {
        return levenshteinDistance;
    }

    public void setLevenshteinDistance(String levenshteinDistance) {
        this.levenshteinDistance = levenshteinDistance;
    }

    public String getAttributeB() {
        return attributeB;
    }

    public void setAttributeB(String attributeB) {
        this.attributeB = attributeB;
    }

    public static void main(String args[]) {
        Connections conn = new Connections();
        Profiler prof = new Profiler("foodmart", "store", conn);
        runSimilarity(conn, prof.getDataSet());
    }

    public static void runSimilarity(Connections conn, Dataset<Row> dataset) {
        String[] columnnames = dataset.columns();
        ArrayList<SemanticSimilarityLevenshtein> similarityList = new ArrayList<>();
        for (String columnA : columnnames) {
            for (String columnB : columnnames) {
                if (columnA.equals(columnB)) {
                } else {
                    ColumnProfiler dtype = new ColumnProfiler();
                    Dataset<Row> df2 = dataset.withColumn("LevenshteinDistance", levenshtein(col(columnA), col(columnB)));
                    List<Row> listLevenshteinDistance = df2.select(col("LevenshteinDistance")).collectAsList();
                    double countDistance = 0;
                    int countValidNumbers = 0;
                    for (Row r : listLevenshteinDistance) {
                        if (r.mkString().equals("null")) {

                        } else {
                            countDistance = countDistance + Double.parseDouble(r.mkString());
                            countValidNumbers = countValidNumbers + 1;
                        }
                    }
                    double levenshteinDistance = (double) countDistance / countValidNumbers;
                    //  df2.show();
                    SemanticSimilarityLevenshtein semanticsimilarityobject = new SemanticSimilarityLevenshtein(columnA, String.valueOf(levenshteinDistance), columnB);
                    similarityList.add(semanticsimilarityobject);
                }
            }

        }
        Encoder<SemanticSimilarityLevenshtein> similaritysemanticEncoder = Encoders.bean(SemanticSimilarityLevenshtein.class);
        Dataset<SemanticSimilarityLevenshtein> dataSetSimilarity = conn.getSession().createDataset(Collections.synchronizedList(similarityList), similaritysemanticEncoder);
        dataSetSimilarity.show();
    }

}
