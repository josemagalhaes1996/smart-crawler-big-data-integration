/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package matcher;

import groovy.sql.DataSet;
import java.io.Serializable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author Utilizador
 */
public class JoinAbility implements Serializable {

    private String columnMain;
    private String columnToCompare;
    private long numberjoins;
    private float percentJoin;

    public JoinAbility(String attributeCompared, String attributeBDW, long numberjoins, float percentJoin) {
        this.columnMain = attributeCompared;
        this.columnToCompare = attributeBDW;
        this.numberjoins = numberjoins;
        this.percentJoin = percentJoin;

    }

    public JoinAbility() {
    }

    public String getColumnMain() {
        return columnMain;
    }

    public void setColumnMain(String columnMain) {
        this.columnMain = columnMain;
    }

    public String getAttributecompared() {
        return columnMain;
    }

    public void setAttributecompared(String attributecompared) {
        this.columnMain = attributecompared;
    }

    public String getColumnToCompare() {
        return columnToCompare;
    }

    public void setColumnToCompare(String columnToCompare) {
        this.columnToCompare = columnToCompare;
    }

    public long getNumberjoins() {
        return numberjoins;
    }

    public void setNumberjoins(long numberjoins) {
        this.numberjoins = numberjoins;
    }

    public float getPercentJoin() {
        return percentJoin;
    }

    public void setPercentJoin(float percentJoin) {
        this.percentJoin = percentJoin;
    }

    public JoinAbility joinabiltymetric(Dataset<Row> datasetCompared, Dataset<Row> dataSetBDW, String attributeCompared, String attributeBDW) {

        //FIQUEI aQUI NO MEU CICLO , E PRECISO TERMINAR ISTO !
        Dataset<Row> joined = datasetCompared.join(dataSetBDW, datasetCompared.col(attributeCompared).equalTo(dataSetBDW.col(attributeBDW)));
       //JACARD DISTANCE

        //FORMULA (2*JOIN/NUMEROTOTAL DE LINHAS DE AMBOS OS DATASOURCES)
        long numJoin = joined.count();
        float percentjoin = 0;
        JoinAbility joinAttributed = new JoinAbility(attributeCompared, attributeBDW, numJoin, percentjoin);

        return joinAttributed;
    }

}
