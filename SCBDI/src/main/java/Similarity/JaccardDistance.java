/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

/**
 *
 * @author Utilizador
 */
public class JaccardDistance {

   private String attributeA;
    private String jarccardDistance;
    private String attributeB;

    public JaccardDistance(String attribute, String distanceValue, String attributebdw) {
        this.attributeA = attribute;
        this.jarccardDistance = distanceValue;
        this.attributeB = attributebdw;
    }

    public String getJarccardDistance() {
        return jarccardDistance;
    }

    public void setJarccardDistance(String jarccardDistance) {
        this.jarccardDistance = jarccardDistance;
    }

 
    public String getAttributeA() {
        return attributeA;
    }

    public void setAttributeA(String attributeA) {
        this.attributeA = attributeA;
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
        List<String> out = new ArrayList<>();
        runJaccardDistance(conn, prof.getDataSet(), prof.getDataSet().columns(), out, 2, 0, prof.getDataSet().columns().length);
    }

    public static void runJaccardDistance(Connections conn, Dataset<Row> dataset, String[] A, List<String> out, int k, int i, int n) {
        if (out.size() == k) {
            if (out.get(0) == out.get(1)) {
            } else {

                org.apache.commons.text.similarity.JaccardDistance jaccardDist = new org.apache.commons.text.similarity.JaccardDistance();
                List<Row> columnA = dataset.select(col(out.get(0))).collectAsList();
                List<Row> columnB = dataset.select(col(out.get(1))).collectAsList();
             //      sim.apply(columnA.toString(), columnB.toString());

                System.out.println(columnA.toString());
                System.out.println(columnB.toString());

                System.out.println("----ColumnMain: " + out.get(0) + "ColumnToCompare: " + out.get(1) + "---Value: " + jaccardDist.apply(columnA.toString(), columnB.toString()));
  
                
            }
            return;
        }
        // start from previous element in the current combination
        // till last element
        for (int j = i; j < n; j++) {
            // add current element A[j] to the solution and recurse with
            // same index j (as repeated elements are allowed in combinations)
            out.add(A[j]);
            runJaccardDistance(conn, dataset, A, out, k, j, n);
            // backtrack - remove current element from solution
            out.remove(out.size() - 1);
//	    code to handle duplicates - skip adjacent duplicate elements
            while (j < n - 1 && A[j] == A[j + 1]) {
                j++;
            }
        }
    }
}
