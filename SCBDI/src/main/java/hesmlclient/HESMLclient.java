/*
 * Copyright (C) 2016 Universidad Nacional de Educación a Distancia (UNED)
 *
 * This program is free software for non-commercial use:
 * you can redistribute it and/or modify it under the terms of the
 * Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International
 * (CC BY-NC-SA 4.0) as published by the Creative Commons Corporation,
 * either version 4 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * section 5 of the CC BY-NC-SA 4.0 License for more details.
 *
 * You should have received a copy of the Creative Commons
 * Attribution-NonCommercial-ShareAlike 4.0 International (CC BY-NC-SA 4.0) 
 * license along with this program. If not,
 * see <http://creativecommons.org/licenses/by-nc-sa/4.0/>.
 *
 */
package hesmlclient;

// Java references
import hesml.taxonomyreaders.wordnet.IWordNetDB;

// HESML references
import hesml.benchmarks.ISimilarityBenchmark;
import hesml.taxonomyreaders.wordnet.impl.WordNetFactory;
import hesml.configurators.*;
import hesml.measures.*;
import hesml.measures.impl.MeasureFactory;
import hesml.taxonomy.*;

/**
 * This class implements a basic client application of the HESML similarity
 * measures library introduced in the paper below.
 *
 * Lastra-Díaz, J. J., and García-Serrano, A. (2016). HESML: a scalable
 * ontology-based semantic similarity measures library with a set of
 * reproducible experiments and a replication dataset. Submitted for publication
 * in Information Systems Journal.
 *
 * @author j.lastra
 */
public class HESMLclient {

    public HESMLclient() {
    }

    /**
     * This function shows how to directly compute the similarity between words
     * without any automatized benchmark. We show the use of two IC-based
     * similarity model with an IC model, as well as the use of two non IC-based
     * similarity measure.
     *
     * @param word1
     * @param word2
     * @return 
     * @throws Exception
     */
    public double[] semanticPairSimilarity(String word1, String word2) throws Exception {

        IWordNetDB wordnet;            // WordNet DB
        ITaxonomy wordnetTaxonomy;    // WordNet taxonomy       
        ISimilarityBenchmark benchmark;  // Benchmark for a single measure       
        IVertexList word1Concepts;  // Vertexes corresponding to the                                  // concepts evoked by the word                                  
        IVertexList word2Concepts;  // Vertexes corresponding to the
        // concepts evoked by the word2                                   

        ITaxonomyInfoConfigurator secoICmodel;        // IC model used

        ISimilarityMeasure WUP;    // Jiang-Conrath Similarity
        ISimilarityMeasure JCN; // Lastra-Díaz and García-Serrano (2015)
        ISimilarityMeasure PATH;          // Mubaid 2009

        
        double[] simValues = new double[3];  // Similarity values

        try {
            // We load the WordNet database
            wordnet = WordNetFactory.loadWordNetDatabase();

            // We build the taxonomy
//            System.out.println("Building the WordNet taxonomy ...");

            wordnetTaxonomy = WordNetFactory.buildTaxonomy(wordnet);

            // We pre-process the taxonomy to compute all the parameters
            // used by the intrinsic IC-computation methods
            wordnetTaxonomy.computesCachedAttributes();

            // We obtain the concepts evoked by the words "shore" and "forest"
            word1Concepts = wordnetTaxonomy.getVertexes().getByIds(
                    wordnet.getWordSynsetsID(word1));

            word2Concepts = wordnetTaxonomy.getVertexes().getByIds(
                    wordnet.getWordSynsetsID(word2));

            // We create all the similarity measures
            JCN = MeasureFactory.getMeasure(wordnetTaxonomy, SimilarityMeasureType.JiangConrath);
            WUP = MeasureFactory.getMeasure(wordnetTaxonomy, SimilarityMeasureType.WuPalmer);
            PATH = MeasureFactory.getMeasure(wordnetTaxonomy, SimilarityMeasureType.PedersenPath);

            // We compute the four similarity values
            simValues[0] = JCN.getHighestPairwiseSimilarity(word1Concepts, word2Concepts);
            simValues[1] = WUP.getHighestPairwiseSimilarity(word1Concepts, word2Concepts);
            simValues[2] = PATH.getHighestPairwiseSimilarity(word1Concepts, word2Concepts);

            // We destroy all resources
            word1Concepts.clear();
            word2Concepts.clear();
            wordnet.clear();
            wordnetTaxonomy.clear();

        } catch (Exception e) {
            simValues = null;
        
        }

        return simValues;

    }

}
