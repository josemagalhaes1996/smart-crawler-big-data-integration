/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AtlasClient;

import com.hortonworks.hwc.Connections;
import java.io.FileWriter;
import java.io.IOException;
import sun.misc.BASE64Encoder;
import java.util.ArrayList;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author Utilizador
 */
public class AtlasConsumer {

    private final String name = "admin";
    private final String password = "adminatlaslid4";
    private final String host = "http://node6.dsi.uminho.pt:21000";

    public AtlasConsumer() {
    }

    public String getName() {
        return name;
    }

    public String getPassword() {
        return password;
    }

    public String getHost() {
        return host;
    }

    public String getDBID(String idTable) throws JSONException {
        JSONObject jsonColumnStatsIDs = entitiesbyType("hive_table");
        for (int i = 0; i < jsonColumnStatsIDs.getJSONArray("results").length(); i++) {
            String entity = (String) jsonColumnStatsIDs.getJSONArray("results").get(i);
            JSONObject jsonTableEntity = getEntity(entity);

            String idTables = jsonTableEntity.getJSONObject("definition").getJSONObject("id").getString("id");
            if (idTables.equalsIgnoreCase(idTable)) {
                return jsonTableEntity.getJSONObject("definition").getJSONObject("values").getJSONObject("db").getString("id");
            }
        }
        return null;
    }

    public String getJob(String qualifiedNameJob) throws JSONException {
        String url = "/api/atlas/discovery/search/dsl?query=Job+where+qualifiedName=" + qualifiedNameJob;
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        javax.ws.rs.client.Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(host + url);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + authStringEnc);
        Response response = invocationBuilder.get();
        String output = response.readEntity(String.class);
        JSONObject obj = new JSONObject(output);
        JSONArray resultsArray = obj.getJSONArray("results");

        for (int i = 0; i < resultsArray.length(); i++) {

            if (resultsArray.getJSONObject(i).get("qualifiedName").equals(qualifiedNameJob) && resultsArray.getJSONObject(i).getJSONObject("$id$").get("state").equals("ACTIVE")) {
                return resultsArray.getJSONObject(i).getJSONObject("$id$").getString("id");
            }
        }
        return null;
    }

    public String getIDColumn(String columnName, String tableName, String dbName) throws JSONException {
        String url = "/api/atlas/discovery/search/dsl?query=hive_column+where+__state=ACTIVE+name=" + columnName + "+table+where+name=" + tableName + "+db+where+name=" + dbName;
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        javax.ws.rs.client.Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(host + url);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + authStringEnc);
        Response response = invocationBuilder.get();
        String output = response.readEntity(String.class);
        
        
        JSONObject obj = new JSONObject(output);
        JSONArray resultsArray = obj.getJSONArray("results");

               
        System.out.println(resultsArray.toString());
       
       
        
        System.out.println(resultsArray.get(0).toString());
        
      for (int i = 0; i < resultsArray.length(); i++) {

            if (resultsArray.getJSONObject(i).getJSONObject("$id$").get("state").equals("ACTIVE")) {
                return resultsArray.getJSONObject(i).getJSONObject("$id$").getString("id");
            }
        }
        return null;

    }
    
    
    public String getIDTables(String tableName, String dbName) throws JSONException {
        String url = "/api/atlas/discovery/search/dsl?query=hive_table+where+__state=ACTIVE+name="+ tableName+"+db+where+name="+dbName;
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        javax.ws.rs.client.Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(host + url);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + authStringEnc);
        Response response = invocationBuilder.get();
        String output = response.readEntity(String.class);
        JSONObject obj = new JSONObject(output);
        JSONArray resultsArray = obj.getJSONArray("results");

        
        
        
  for (int i = 0; i < resultsArray.length(); i++) {

            if (resultsArray.getJSONObject(i).getJSONObject("$id$").get("state").equals("ACTIVE")) {
                return resultsArray.getJSONObject(i).getJSONObject("$id$").getString("id");
            }
        }
        return null;
    }
    

    public ArrayList<String> getColumnStatsID(String tableID) throws JSONException {
        JSONObject jsonColumnStatsIDs = entitiesbyType("ColumnStatistics");
        ArrayList<String> arraycolumnID = new ArrayList<>();
        for (int i = 0; i < jsonColumnStatsIDs.getJSONArray("results").length(); i++) {
            String entity = (String) jsonColumnStatsIDs.getJSONArray("results").get(i);
            JSONObject jsonColumnEntity = getEntity(entity);
            String columnName = jsonColumnEntity.getJSONObject("definition").getJSONObject("values").getString("name");
            String idColumnStats = jsonColumnEntity.getJSONObject("definition").getJSONObject("id").getString("id");

            String idTable = jsonColumnEntity.getJSONObject("definition").getJSONObject("values").getJSONObject("tableName").getString("id");

            if (idTable.equalsIgnoreCase(tableID)) {
                arraycolumnID.add(idColumnStats);
            }
        }
        return arraycolumnID;
    }

    public String getIDColumnStatistics(String idTable, String columnName) throws JSONException {
        JSONObject jsonColumn = entitiesbyType("ColumnStatistics");
        for (int i = 0; i < jsonColumn.getJSONArray("results").length(); i++) {
            String entity = (String) jsonColumn.getJSONArray("results").get(i);
            JSONObject jsonColumnEntity = getEntity(entity);
            String status = jsonColumnEntity.getJSONObject("definition").getJSONObject("id").getString("state");
            String idTableEntity = jsonColumnEntity.getJSONObject("definition").getJSONObject("values").getJSONObject("tableName").getString("id");
            String nameColumn = jsonColumnEntity.getJSONObject("definition").getJSONObject("values").getString("name");
            if (idTable.equalsIgnoreCase(idTableEntity) && status.equalsIgnoreCase("ACTIVE") && nameColumn.equalsIgnoreCase(columnName)) {
                return jsonColumnEntity.getJSONObject("definition").getJSONObject("id").getString("id");
            }
        }
        return null;
    }

    public static void main(String[] args) throws IOException, JSONException {
        Connections conn = new Connections();
        AtlasConsumer consumer = new AtlasConsumer();

        consumer.getEntity("dc68aff6-4460-4405-baf5-c9c9c5fb649f");
        String id = consumer.getDBID("dc68aff6-4460-4405-baf5-c9c9c5fb649f");
        String id2 = consumer.getIDAtlasColumnACTIVE("full_name", "brancha", "josedb");
        String id3 = consumer.getIDAtlasTableACTIVE("brancha", "josedb");

        try (FileWriter file = new FileWriter("teste2.json")) {
            file.write(id2);
            System.out.println("Successfully Copied JSON Object to File...");
            System.out.println("\nJSON Object: " + id3);
        } catch (Exception ex) {
            ex.getMessage();
        }
    }

    public String getIDTableStatistics(String idTable) throws JSONException {
        JSONObject jsonTable = entitiesbyType("TableStatistics");
        for (int i = 0; i < jsonTable.getJSONArray("results").length(); i++) {
            String entity = (String) jsonTable.getJSONArray("results").get(i);
            JSONObject jsonTableEntity = getEntity(entity);
            String status = jsonTableEntity.getJSONObject("definition").getJSONObject("id").getString("state");
            String idTableEntity = jsonTableEntity.getJSONObject("definition").getJSONObject("values").getJSONObject("table").getString("id");
            if (idTable.equalsIgnoreCase(idTableEntity) && status.equalsIgnoreCase("ACTIVE")) {
                return jsonTableEntity.getJSONObject("definition").getJSONObject("id").getString("id");
            }
        }
        return null;

    }

    public String getIDAtlasColumnACTIVE(String column, String table, String database) throws JSONException {
        JSONObject jsonHive = entitiesbyType("hive_column");

        for (int i = 0; i < jsonHive.getJSONArray("results").length(); i++) {
            String entity = (String) jsonHive.getJSONArray("results").get(i);
            JSONObject jsonColumnEntity = getEntity(entity);
            String columnName = jsonColumnEntity.getJSONObject("definition").getJSONObject("values").getString("name");
            String state = jsonColumnEntity.getJSONObject("definition").getJSONObject("id").getString("state");
            String idTable = jsonColumnEntity.getJSONObject("definition").getJSONObject("values").getJSONObject("table").getString("id");

            JSONObject jsonTableEntity = getEntity(idTable);
            String tableName = jsonTableEntity.getJSONObject("definition").getJSONObject("values").getString("name");
            String idDataBase = jsonTableEntity.getJSONObject("definition").getJSONObject("values").getJSONObject("db").getString("id");

            JSONObject jsonDBEntity = getEntity(idDataBase);
            String nameDB = jsonDBEntity.getJSONObject("definition").getJSONObject("values").getString("name");

            if (tableName.equalsIgnoreCase(table) && columnName.equalsIgnoreCase(column) && nameDB.equalsIgnoreCase(database) && state.equalsIgnoreCase("ACTIVE")) {

                return jsonColumnEntity.getJSONObject("definition").getJSONObject("id").getString("id");
            }
        }
        return null;

    }

    public String getIDAtlasTableACTIVE(String name, String database) throws JSONException {

        org.json.JSONObject jsonHive = entitiesbyType("hive_table");
        for (int i = 0; i < jsonHive.getJSONArray("results").length(); i++) {
            String entity = (String) jsonHive.getJSONArray("results").get(i);
            JSONObject jsonHiveTableEntity = getEntity(entity);
            String idDataBase = jsonHiveTableEntity.getJSONObject("definition").getJSONObject("values").getJSONObject("db").getString("id");
            JSONObject jsonDBEntity = getEntity(idDataBase);
            if (jsonHiveTableEntity.getJSONObject("definition").getJSONObject("id").getString("state").equals("ACTIVE") && jsonHiveTableEntity.getJSONObject("definition").getJSONObject("values").getString("name").equals(name) && jsonDBEntity.getJSONObject("definition").getJSONObject("values").getString("name").equals(database)) {
                return jsonHiveTableEntity.getJSONObject("definition").getJSONObject("id").getString("id");
            }
        }
        return null;

    }

    public JSONObject getAllTypes() throws JSONException, IOException {
        String url = "/api/atlas/types";
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        javax.ws.rs.client.Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(host + url);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + authStringEnc);

        Response response = invocationBuilder.get();
        String output = response.readEntity(String.class
        );

        System.out.println(response.toString());
        JSONObject obj = new JSONObject(output);

        return obj;
    }

    public JSONObject entitiesbyType(String type) throws JSONException {
        String url = "/api/atlas/entities?type=" + type;
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        javax.ws.rs.client.Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(host + url);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + authStringEnc);

        Response response = invocationBuilder.get();
        String output = response.readEntity(String.class
        );
        System.out.println(response.toString());
        JSONObject obj = new JSONObject(output);
        return obj;

    }

    public JSONObject getEntity(String entity) throws JSONException {
        String url = "/api/atlas/entities/" + entity;
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        javax.ws.rs.client.Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(host + url);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + authStringEnc);
        Response response = invocationBuilder.get();
        String output = response.readEntity(String.class);
        JSONObject obj = new JSONObject(output);
        return obj;
    }

    public void createEntityAtlas(JSONObject json) throws Exception {
        DefaultHttpClient httpclient = new DefaultHttpClient();
        HttpPost httpost = new HttpPost(host + "/api/atlas/entities");

        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());

        StringEntity se = new StringEntity(json.toString());
        httpost.setEntity(se);
        httpost.setHeader("Authorization", "Basic " + authStringEnc);
        httpost.setHeader("Accept", "application/json");
        httpost.setHeader("Content-type", "application/json");

        ResponseHandler responseHandler = new BasicResponseHandler();
        Object response = httpclient.execute(httpost, responseHandler);

    }

    public JSONObject getGuidHiveTable(String nameEntity, String type) throws JSONException {
        String url = "/api/atlas/discovery/search/dsl?query=" + type + "+where+__state=ACTIVE+name=" + nameEntity;
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        javax.ws.rs.client.Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(host + url);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + authStringEnc);
        Response response = invocationBuilder.get();
        String output = response.readEntity(String.class
        );
        System.out.println(response.toString());
        JSONObject obj = new JSONObject(output);
        return obj;
    }

    public JSONArray getAudtisTable(String guidTable) throws JSONException {
        String url = "/api/atlas/v2/entity/" + guidTable + "/audit";
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        javax.ws.rs.client.Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(host + url);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + authStringEnc);

        Response response = invocationBuilder.get();
        String output = response.readEntity(String.class
        );
        System.out.println(response.toString());
        JSONArray obj = new JSONArray(output);
        return obj;

    }

    public void updateAuditsTable(int audits, String guid) throws IOException {
        DefaultHttpClient httpclient = new DefaultHttpClient();
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());

        HttpPut httput = new HttpPut(host + "/api/atlas/v2/entity/guid/" + guid + "?name=acessesCounter");
        StringEntity se = new StringEntity(String.valueOf(audits));
        httput.setEntity(se);
        httput.setHeader("Authorization", "Basic " + authStringEnc);
        httput.setHeader("Accept", "application/json");
        httput.setHeader("Content-type", "application/json");

        ResponseHandler responseHandler = new BasicResponseHandler();
        Object response = httpclient.execute(httput, responseHandler);

    }
}
