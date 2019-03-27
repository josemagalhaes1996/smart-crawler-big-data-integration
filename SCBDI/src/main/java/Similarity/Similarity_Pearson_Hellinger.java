/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 *
 * @author Utilizador
 */
public class Similarity_Pearson_Hellinger {

    public Similarity_Pearson_Hellinger() {
    }

    //Distancia de Hellinger  entre valores 
    public static void main(String args[]) {

        Vector dv = Vectors.dense(1.0, 0.0, 3.0);
        Vector dv2 = Vectors.dense(23.0, 2424.0, 4.0);
        Similarity_Pearson_Hellinger a = new Similarity_Pearson_Hellinger();

        System.out.println("É a Distance de Similarity HellingerDistance " + a.hellingerDistance(dv, dv2));
        System.out.println("É a Distance de Similarity pearsonDistance" + a.pearsonDistance(dv, dv2));

        // RowMatrix v  = new RowMatrix();
    }

    public double hellingerDistance(Vector vecA, Vector vecB) {
        double[] arrA = vecA.toArray();
        double[] arrB = vecB.toArray();

        double sim = 0.0;

        int arrsize = arrA.length;
        for (int i = 0; i < arrsize; i++) {
            double a = arrA[i];
            double b = arrB[i];
            double sqrtDiff = Math.sqrt(a) - Math.sqrt(b);
            sim += sqrtDiff * sqrtDiff;
        }

        return (sim / Math.sqrt(2));

    }

    /**
     * Calculate similarity (Pearson Distance) between vectors
     *
     * @param vecA initial vector from which to calculate a similarity
     * @param vecB second vector involved in similarity calculation
     * @return similarity between two vectors
     */
    public double pearsonDistance(Vector vecA, Vector vecB) {
        double[] arrA = vecA.toArray();
        double[] arrB = vecB.toArray();

        int viewA = 0;
        int viewB = 0;
        int viewAB = 0;

        int arrsize = arrA.length;
        for (int i = 0; i < arrsize; i++) {
            if (arrA[i] > 0) {
                viewA++;
            }

            if (arrB[i] > 0) {
                viewB++;
            }

            if (arrB[i] > 0 && arrA[i] > 0) {
                viewAB++;
            }
        }
        return (viewAB / (Math.sqrt(viewA) * Math.sqrt(viewB)));
    }
}
