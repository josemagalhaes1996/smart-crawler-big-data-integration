/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Similarity;

import edu.uniba.di.lacam.kdde.lexical_db.ILexicalDatabase;
import edu.uniba.di.lacam.kdde.lexical_db.MITWordNet;
import edu.uniba.di.lacam.kdde.ws4j.RelatednessCalculator;
import edu.uniba.di.lacam.kdde.ws4j.similarity.HirstStOnge;
import edu.uniba.di.lacam.kdde.ws4j.similarity.JiangConrath;
import edu.uniba.di.lacam.kdde.ws4j.similarity.LeacockChodorow;
import edu.uniba.di.lacam.kdde.ws4j.similarity.Lesk;
import edu.uniba.di.lacam.kdde.ws4j.similarity.Lin;
import edu.uniba.di.lacam.kdde.ws4j.similarity.Path;
import edu.uniba.di.lacam.kdde.ws4j.similarity.Resnik;
import edu.uniba.di.lacam.kdde.ws4j.similarity.WuPalmer;
import edu.uniba.di.lacam.kdde.ws4j.util.WS4JConfiguration;

/**
 *
 * @author Utilizador
 */
public class OntologySimilarity {
    
    
     private static RelatednessCalculator[] rcs;

    static {
        WS4JConfiguration.getInstance().setMemoryDB(false);
        WS4JConfiguration.getInstance().setMFS(true);
        ILexicalDatabase db = new MITWordNet();
        rcs = new RelatednessCalculator[]{
            new HirstStOnge(db), new LeacockChodorow(db), new Lesk(db), new WuPalmer(db),
            new Resnik(db), new JiangConrath(db), new Lin(db), new Path(db)
        };
    }

    public static void main(String[] args) {

        String[] words = {"add", "get", "filter", "remove", "check", "find", "collect", "create"};

//        for (int i = 0; i < words.length - 1; i++) {
//            for (int j = i + 1; j < words.length; j++) {
        for (int d = 0; d < rcs.length; d++) {

            System.out.println("\t" + rcs[d].getClass().getName() + " = " + rcs[d].calcRelatednessOfWords("store", "shop"));

//                }
//              
//               
//            }
        }
//                
//        long t = System.currentTimeMillis();
//        Arrays.asList(rcs).forEach(rc -> System.out.println(rc.getClass().getName() + "\t"
//                + rc.calcRelatednessOfWords("car", "car")));
//        System.out.println("\nDone in " + (System.currentTimeMillis() - t) + " msec.");
    }
}
