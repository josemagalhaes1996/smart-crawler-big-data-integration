/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AtlasClient;

import com.hortonworks.hwc.Connections;
import java.util.Iterator;
import java.util.Map;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONObject;
import sun.misc.BASE64Encoder;

/**
 *
 * @author Utilizador
 */
public class Rest {

    public Rest() {
    }

    public  String makeRequest(String path) throws Exception {

      DefaultHttpClient httpclient = new DefaultHttpClient();
      HttpPost httpost = new HttpPost(path);
      String data = " {\"jsonClass\":\"org.apache.atlas.typesystem.json.InstanceSerialization$_Reference\",\"id\":{\"jsonClass\":\"org.apache.atlas.typesystem.json.InstanceSerialization$_Id\",\"id\":\"-1466683608564093000\",\"version\":0,\"typeName\":\"TableStatistics\"},\"typeName\":\"TableStatistics\",\"values\":{\"name\":\"forestfires\",\"owner\":\"rajops\",\"description\":\"Profiling attribute + attribute from + table\",\"qualifiedName\":\"st.profiler.forestfires\",\"table\":{\"jsonClass\":\"org.apache.atlas.typesystem.json.InstanceSerialization$_Id\",\"id\":\"01ba6b2d-6170-41f3-baa6-eb58a4aecf30\",\"version\":0,\"typeName\":\"hive_table\",\"state\":\"ACTIVE\"},\"db\":{\"jsonClass\":\"org.apache.atlas.typesystem.json.InstanceSerialization$_Id\",\"id\":\"843b4064-0e58-4bbe-9c64-bc2b93ee7bfd\",\"version\":0,\"typeName\":\"hive_db\",\"state\":\"ACTIVE\"},\"columnStatistics\":[],\"numCategoricalColumns\":2,\"numDateColumns\":3,\"numObservations\":2,\"numVariables\":2,\"numNumericalColumns\":2,\"numOtherTypesColumns\":3},\"traitNames\":[],\"traits\":{}}";

        JSONObject holder = new JSONObject(data);
        String authString = "admin" + ":" + "adminatlaslid4";
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
      

        StringEntity se = new StringEntity(holder.toString());
        httpost.setEntity(se);
        httpost.setHeader("Authorization", "Basic " + authStringEnc);
        httpost.setHeader("Accept", "application/json");
        httpost.setHeader("Content-type", "application/json");

        ResponseHandler responseHandler = new BasicResponseHandler();
        Object response = httpclient.execute(httpost, responseHandler);
        return response.toString();
    }
    
    public static void main(String args[]) throws Exception{
    Connections conn = new Connections();
    Rest restc = new Rest();
    
    restc.makeRequest("http://node6.dsi.uminho.pt:21000/api/atlas/entities");
        
    }
    
    
}
