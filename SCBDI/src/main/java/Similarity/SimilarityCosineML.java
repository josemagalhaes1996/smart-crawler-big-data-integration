/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import com.hortonworks.hwc.Connections;
import basicProfiler.ColumnProfiler;
import basicProfiler.Profiler;
import java.util.ArrayList;
import java.util.Arrays;
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
        Profiler prof = new Profiler("tpcds", "item", conn);
        runCosineSimilarity(conn, prof.getDataSet());
    }

    public static void runCosineSimilarity(Connections conn, Dataset<Row> dataset) {
        String[] columnnames = dataset.columns();
        ArrayList<SimilarityCosineML> SimilarityCosineList = new ArrayList<>();
        for (String columnA : columnnames) {
            System.out.println("FOR to column " + columnA);
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
                
                        Dataset<Row> newDataSet = dataset.sample(0.1);
                        newDataSet.select(columnA).na().fill(0);
                        newDataSet.select(columnB).na().fill(0);

                        List<Row> listdoublecolumnA = newDataSet.select(columnA).collectAsList();
                        List<Row> listdoublecolumnB = newDataSet.select(columnB).collectAsList();
                        List<Double> listdoublecolumnB1 = newDataSet.select(columnB).as(Encoders.DOUBLE()).collectAsList();

                        
                        double[] doublelistcolumnA = new double[listdoublecolumnA.size()];
                        double[] doublelistcolumnB = new double[listdoublecolumnB.size()];
                            System.out.println("FOR to make convert the column in vector");
                        
                                    
//                        for (int i = 0; i < listdoublecolumnA.size(); i++) {
//                            if (i == 0) {
//                                System.out.println("FOR to make convert the column in vector");
//
//                            }
//                            if (listdoublecolumnA.get(i).mkString().equalsIgnoreCase("null") || listdoublecolumnB.get(i).mkString().equalsIgnoreCase("null")) {
//                            } else {
//                                doublelistcolumnA[i] = Double.parseDouble(listdoublecolumnA.get(i).mkString());
//                                doublelistcolumnB[i] = Double.parseDouble(listdoublecolumnB.get(i).mkString());
//                            }
//                        }
                        
                        Vector vectorcolumnA = org.apache.spark.mllib.linalg.Vectors.parse("1,2,3,4,20,32");
                    //    Vector vectorcolumnB = org.apache.spark.mllib.linalg.Vectors.parse(listdoublecolumnB1.toString());
                        List<Vector> dist = new ArrayList<>();
                        dist.add(vectorcolumnA);
                        dist.add(vectorcolumnA);
                        
                        
                        JavaRDD<Vector> rdd = conn.getJavasparkContext().parallelize(dist);
                        RowMatrix mat = new RowMatrix(rdd.rdd());

                        double valueSimilarity = mat.columnSimilarities().entries().first().value();
                        System.out.println("\n" + "\t");
                        System.out.println("ColumnB : " + columnB + " ValueofSimilarity: " + valueSimilarity);
                        SimilarityCosineML similarityCosineObject = new SimilarityCosineML(columnA, String.valueOf(valueSimilarity), columnB);
                        SimilarityCosineList.add(similarityCosineObject);
                    }
                }
            }
        }
        Encoder<SimilarityCosineML> similarityCosineEncoder = Encoders.bean(SimilarityCosineML.class);
        Dataset<SimilarityCosineML> dataSetCorrelation = conn.getSession().createDataset(Collections.synchronizedList(SimilarityCosineList), similarityCosineEncoder);
        dataSetCorrelation.show();
    }

}
