/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import org.apache.commons.text.similarity.JaccardDistance;
import org.apache.commons.text.similarity.JaccardSimilarity;
import org.apache.commons.text.similarity.JaccardSimilarity;

/**
 *
 * @author Utilizador
 */
public class CommonsSimilarity {
    
    
    public static void main(String args[]){
    
    
       JaccardDistance n = new JaccardDistance(); 
       
       n.apply("teste", "teste44");
    
       JaccardSimilarity sim = new JaccardSimilarity();
       
       sim.apply("shop", "store");
    
    
    }
    
     
}
