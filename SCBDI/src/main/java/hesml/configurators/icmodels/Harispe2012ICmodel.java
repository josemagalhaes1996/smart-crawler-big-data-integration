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

package hesml.configurators.icmodels;

// HESML references

import hesml.taxonomy.*;
import hesml.configurators.ITaxonomyInfoConfigurator;
import hesml.configurators.IntrinsicICModelType;

/**
 * This function implements an unpublished method developed by Harispe as a
 * modification of the Sánchez et al. (2011) IC model. It is included in
 * the SML library and we reproduced here with the aim of evaluating in our
 * benchmarks in Lastra-Díaz and García-Serrano (2015).
 * If you use this IC model in your research, you must cite the SML paper
 * introduced by Harispe et al. that is shown below.
 * 
 * Harispe, S., Ranwez, S., Janaqi, S., and Montmain, J. (2014).
 * The semantic measures library and toolkit: fast computation of semantic
 * similarity and relatedness using biomedical ontologies.
 * Bioinformatics, 30(5), 740–742.
 * 
 * Lastra-Díaz, J. J., and García-Serrano, A. (2015).
 * A novel family of IC-based similarity measures with a detailed experimental
 * survey on WordNet. Engineering Applications of Artificial Intelligence
 * Journal, 46, 140–153.
 * 
 * @author Juan Lastra-Díaz
 */

class Harispe2012ICmodel extends AbstractICmodel
    implements ITaxonomyInfoConfigurator
{
    /**
     * We set the ICvalues for each node.
     * @param taxonomy Taxonomy whose IC model will be computed
     * @throws Exception 
     */
    
    @Override
    public void setTaxonomyData(ITaxonomy taxonomy) throws Exception
    {
        double  icValue;    // Returned value
        
        double  maxLeaves = taxonomy.getVertexes().getLeavesCount();
        
        double  log2 = Math.log(2.0);   // Logarithm of 2
        
        double  prob;   // Estimation of the probability (Harispe 2012)
        
        double  leaves;     // Number of leaf nodes without including the node
        double  subSummers;  // Set of ancestors including the node
        
        // We compute the IC value for each node
        
        for (IVertex vertex: taxonomy.getVertexes())
        {
            // We get the leaves and hyponyms of the node
            
            leaves = vertex.getNonInclusiveSubsumedLeafSetCount();
            subSummers = vertex.getNonInclusiveAncestorSetCount() + 1;
                    
            // We compute the estimation for the probability of the node
            
            prob = (leaves + 1.0 / subSummers) / maxLeaves;
            
            // We compute the IC-value of [harispe, 2012] in SML
            
            icValue = -Math.log(prob) / log2;
            
            // We save the IC-value in the vertex
            
            vertex.setICValue(icValue);
        }
        
        // We set the delta IC weights for the weighted Measures
        
        setICDeltaWeights(taxonomy);
    }
    
    /**
     * This function returns a String representing IC model type of the
     * current object.
     * @return A string representing the IC model type of the current instance
     */
    
    @Override
    public String toString()
    {
        return (IntrinsicICModelType.Harispe.toString());
    }        
}
