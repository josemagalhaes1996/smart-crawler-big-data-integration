/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import org.apache.commons.text.similarity.JaccardDistance;
import org.apache.commons.text.similarity.JaccardSimilarity;


/**
 *
 * @author Utilizador
 */
public class Test {
    public static void main(String args[]) {

        JaccardDistance n = new JaccardDistance();
        n.apply("teste", "teste44");        
        JaccardSimilarity sim = new JaccardSimilarity();
           System.out.println(sim.apply("shop", "store"));
        
        

    }
}
