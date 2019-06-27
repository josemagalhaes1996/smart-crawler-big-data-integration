/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import static Similarity.SimilarityDistributed.similirtyAnalysis;
import com.hortonworks.hwc.Connections;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 *
 * @author Utilizador
 */
public class SimilarityIntersection {
     
    
    public static void main(String args[]) throws IOException {
        Instant start = Instant.now();
        Connections conn = new Connections();
        similirtyAnalysis("tpcds", "promotion", "store_sales");
        Instant end = Instant.now().minus(start.getEpochSecond(), ChronoUnit.SECONDS);
        System.out.println("O JOb demorou " + end.getEpochSecond() + " Segundos");

    }
}
