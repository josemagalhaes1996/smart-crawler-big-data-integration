/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package advancedProfiler;

import com.hortonworks.hwc.Connections;
import basicProfiler.ColumnProfiler;
import basicProfiler.Profiler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

/**
 *
 * @author Utilizador
 */
public class SimilarityCosineML {

    private String attributeA;
    private String cosineSimilarityValue;
    private String attributeB;

    public SimilarityCosineML(String attributeA, String cosineSimilarityValue, String attributeB) {
        this.attributeA = attributeA;
        this.cosineSimilarityValue = cosineSimilarityValue;
        this.attributeB = attributeB;
    }

    public SimilarityCosineML() {
    }

    public String getAttributeA() {
        return attributeA;
    }

    public void setAttributeA(String attributeA) {
        this.attributeA = attributeA;
    }

    public String getCosineSimilarityValue() {
        return cosineSimilarityValue;
    }

    public void setCosineSimilarityValue(String cosineSimilarityValue) {
        this.cosineSimilarityValue = cosineSimilarityValue;
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
        runCosineSimilarity(conn, prof.getDataSet());
    }

    public static void runCosineSimilarity(Connections conn, Dataset<Row> dataset) {
        String[] columnnames = dataset.columns();
        ArrayList<SimilarityCosineML> SimilarityCosineList = new ArrayList<>();
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
                        List<Row> listdoublecolumnA = dataset.select(columnA).collectAsList();
                        List<Row> listdoublecolumnB = dataset.select(columnB).collectAsList();

                        double[] doublelistcolumnA = new double[listdoublecolumnA.size()];
                        double[] doublelistcolumnB = new double[listdoublecolumnB.size()];

                        for (int i = 0; i < listdoublecolumnA.size(); i++) {
                            if(listdoublecolumnA.get(i).mkString().equalsIgnoreCase("null") ||listdoublecolumnB.get(i).mkString().equalsIgnoreCase("null")){
                            }else{
                            doublelistcolumnA[i] = Double.parseDouble(listdoublecolumnA.get(i).mkString());
                            doublelistcolumnB[i] = Double.parseDouble(listdoublecolumnB.get(i).mkString());
                        }
                        }
                        Vector vectorcolumnA = org.apache.spark.mllib.linalg.Vectors.dense(doublelistcolumnA);
                        Vector vectorcolumnB = org.apache.spark.mllib.linalg.Vectors.dense(doublelistcolumnB);
                        List<Vector> dist = new ArrayList<>();
                        dist.add(vectorcolumnA);
                        dist.add(vectorcolumnB);
                        JavaRDD<Vector> rdd = conn.getJavasparkContext().parallelize(dist);
                        RowMatrix mat = new RowMatrix(rdd.rdd());
                        CoordinateMatrix cord = mat.columnSimilarities();
                        double valueSimilarity= mat.columnSimilarities().entries().first().value();

                        SimilarityCosineML similarityCosineObject = new SimilarityCosineML(columnA, String.valueOf(valueSimilarity), columnB);
                        SimilarityCosineList.add(similarityCosineObject);
                    }
                }
            }
        }
  Encoder<SimilarityCosineML> similarityCosineEncoder = Encoders.bean(SimilarityCosineML.class);
    Dataset<SimilarityCosineML> dataSetCorrelation = conn.getSession().createDataset(Collections.synchronizedList(SimilarityCosineList), similarityCosineEncoder);

    dataSetCorrelation.show ();
    }
  
}

