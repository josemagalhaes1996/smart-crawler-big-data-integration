/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package advancedProfiler;

import static advancedProfiler.CorrelationAnalysis.runCorrelations;
import basicProfiler.Profiler;
import com.hortonworks.hwc.Connections;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Utilizador
 */
public class Combination {
    // Function to print all distinct combinations of length k where
    // repetition of elements is allowed
    public static void recurse(String[] A, List<String> out, int k, int i, int n , ArrayList<String[]> listfunction) {
       
        // base case: if combination size is k, print it
        
        if(listfunction == null){
               listfunction = new ArrayList<>();

        }
        
        if (out.size() == k) {
            if (out.get(0) == out.get(1)) {
            } else {
//                System.out.println(out);
                 listfunction.add(out.toArray(new String[2]));
//                System.out.println(listfunction.size());
            }
            return;
        }
        // start from previous element in the current combination
        // till last element
        for (int j = i; j < n; j++) {
            // add current element A[j] to the solution and recurse with
            // same index j (as repeated elements are allowed in combinations)
            out.add(A[j]);
            recurse(A, out, k, j, n,listfunction);
            

            // backtrack - remove current element from solution
            out.remove(out.size() - 1);
//			 code to handle duplicates - skip adjacent duplicate elements
            while (j < n - 1 && A[j] == A[j + 1]) {
                j++;
            }
        }
    }

    // main function
    public static void main(String[] args) {
        Connections conn = new Connections();
        Profiler prof = new Profiler("tpcds", "store_sales", conn);
       
        int k = 2;
        // if array contains repeated elements, sort the array to
        // handle duplicates combinations
     
     List<String> out = new ArrayList<>(); 
         ArrayList<String[]>  finallist = new ArrayList<>();

     recurse(prof.getDataSet().columns(), out, 2, 0, prof.getDataSet().columns().length, finallist);

        System.out.println(finallist.size());
        for (int i = 0 ; i < finallist.size();i++){
            System.out.println("Par---" + i + "  ------- Attribute1---- " + finallist.get(i)[0] + "  ------Atribute 2:----  " + finallist.get(i)[1]);
            System.out.println("\n");
        }
       
    }
}
