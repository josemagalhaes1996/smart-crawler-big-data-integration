/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package matcher;

import basicProfiler.ColumnProfiler;
import com.hortonworks.hwc.Connections;
import basicProfiler.Profiler;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

/**
 *
 * @author Utilizador
 */
public class JoinAbilityCompare implements Serializable {

    public static void main(String args[]) {
        Connections conn = new Connections();
        Profiler prof = new Profiler("foodmart", "store", conn); //THIS tABLE is a TABLE that arrives to BDW
        Profiler prof2 = new Profiler("foodmart", "region", conn);
        runJoinAbility(conn, prof.getDataSet(), prof2.getDataSet());
    }

    public static void runJoinAbility(Connections conn, Dataset<Row> datasetarrived, Dataset<Row> datasetBDW) {
        String[] columnnames = datasetarrived.columns();
        List<JoinAbility> joinedlist = new ArrayList<>();
        //Só é possivel fazer o joinse o Tipo de Dados for 

        for (String columnstocompare : columnnames) {
            for (String columnBDW : datasetBDW.columns()) {
                ColumnProfiler datatypecolumn = new ColumnProfiler();
                String dtypeAttributeComprared = datatypecolumn.dataTypeColumn(datasetarrived.dtypes(), columnstocompare);
                String dtypeAttributeBDW = datatypecolumn.dataTypeColumn(datasetBDW.dtypes(), columnBDW);
                if (dtypeAttributeComprared.equals(dtypeAttributeBDW)) {
                    JoinAbility joinattribute = new JoinAbility();
                    JoinAbility joinedCheckDS = joinattribute.joinabiltymetric(datasetarrived, datasetBDW, columnstocompare, columnBDW);
                    joinedCheckDS.setPercentJoin((float) (((float) 2 * joinedCheckDS.getNumberjoins()) / (2 * datasetarrived.count())) * 100);
                    joinedlist.add(joinedCheckDS);
                }
            }
        }
        Encoder<JoinAbility> columnEncoder = Encoders.bean(JoinAbility.class);
        Dataset<JoinAbility> joinCheckDS = conn.getSession().createDataset(Collections.synchronizedList(joinedlist), columnEncoder);
        joinCheckDS.show();
    }
}
