/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package atlasClient;

import sun.misc.BASE64Encoder;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import jdk.nashorn.internal.parser.JSONParser;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.json.JSONArray;

/**
 *
 * @author Utilizador
 */
public class AtlasConsumer {

    private final String name = "admin";
    private final String password = "adminatlaslid4";

    public AtlasConsumer() {
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

    public static void main(String args[]) throws JSONException {
 AtlasConsumer consumer = new AtlasConsumer();

        System.out.println(consumer.getIDAtlasColumnACTIVE("ssn", "branch_intersect", "default"));
        System.out.println(consumer.getIDAtlasColumnACTIVE("location", "branch_intersect", "default"));
        System.out.println(consumer.getIDAtlasTableACTIVE("branch_intersect", "default"));

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
        JSONObject jsonHive = entitiesbyType("hive_table");
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

    public JSONObject getAllTypes() throws JSONException {
        String url = "http://node6.dsi.uminho.pt:21000/api/atlas/types";
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        Client restClient = Client.create();
        WebResource webResource = restClient.resource(url);
        ClientResponse resp = webResource.accept("application/json")
                .header("Authorization", "Basic " + authStringEnc)
                .get(ClientResponse.class);
        if (resp.getStatus() != 200) {
            System.err.println("Unable to connect to the server");
        }
        String output = resp.getEntity(String.class);
        JSONObject jsonObj = new JSONObject(output);

        return jsonObj;

    }

    public JSONObject getEntity(String entity) throws JSONException {
        String url = "http://node6.dsi.uminho.pt:21000/api/atlas/entities/" + entity;
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        Client restClient = Client.create();
        WebResource webResource = restClient.resource(url);
        ClientResponse resp = webResource.accept("application/json")
                .header("Authorization", "Basic " + authStringEnc)
                .get(ClientResponse.class);
        if (resp.getStatus() != 200) {
            System.err.println("Unable to connect to the server");
        }
        String output = resp.getEntity(String.class);
        JSONObject jsonObj = new JSONObject(output);
        return jsonObj;

    }

    public JSONObject entitiesbyType(String type) throws JSONException {
        String url = "http://node6.dsi.uminho.pt:21000/api/atlas/entities?type=" + type;
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        Client restClient = Client.create();
        WebResource webResource = restClient.resource(url);
        ClientResponse resp = webResource.accept("application/json")
                .header("Authorization", "Basic " + authStringEnc)
                .get(ClientResponse.class);
        if (resp.getStatus() != 200) {
            System.err.println("Unable to connect to the server");
        }
        String output = resp.getEntity(String.class);
        JSONObject jsonObj = new JSONObject(output);
        return jsonObj;
    }

    public void createEntityAtlas(org.json.simple.JSONObject json) {
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        try {
            Client client = Client.create();
            WebResource webResource = client.resource("http://node6.dsi.uminho.pt:21000/api/atlas/entities");
            ClientResponse response = webResource.type("application/json").header("Authorization", "Basic " + authStringEnc)
                    .post(ClientResponse.class, json.toJSONString());
            if (response.getStatus() != 201) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + response.getStatus());
            }
            System.out.println("Output from Server .... \n");
            String output = response.getEntity(String.class);
            System.out.println(output);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String createEntity(org.json.simple.JSONObject json) {
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        try {
            Client client = Client.create();
            WebResource webResource = client.resource("http://node6.dsi.uminho.pt:21000/api/atlas/entities");
            ClientResponse response = webResource.type("application/json").header("Authorization", "Basic " + authStringEnc)
                    .post(ClientResponse.class, json.toJSONString());
            if (response.getStatus() != 201) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + response.getStatus());
            }
            System.out.println("Output from Server .... \n");
            String output = response.getEntity(String.class);
            System.out.println(output);
            return output;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String createType() {
        String authString = name + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        try {
            String type = "{\n"
                    + "    \"enumTypes\": [],\n"
                    + "    \"structTypes\": [],\n"
                    + "    \"traitTypes\": [],\n"
                    + "    \"classTypes\": [{\n"
                    + "            \"superTypes\": [\"DataSet\"],\n"
                    + "            \"hierarchicalMetaTypeName\": \"org.apache.atlas.typesystem.types.ClassType\",\n"
                    + "            \"typeName\": \"statistics_hive3\",\n"
                    + "            \"typeDescription\": null,\n"
                    + "            \"attributeDefinitions\": [{\n"
                    + "                    \"name\": \"id\",\n"
                    + "                    \"dataTypeName\": \"string\",\n"
                    + "                    \"multiplicity\": \"required\",\n"
                    + "                    \"isComposite\": false,\n"
                    + "                    \"isUnique\": true,\n"
                    + "                    \"isIndexable\": true,\n"
                    + "                    \"reverseAttributeName\": null\n"
                    + "                }\n"
                    + "            ]\n"
                    + "        }\n"
                    + "    ]\n"
                    + "}";
            Client client = Client.create();
            WebResource webResource = client.resource("http://node6.dsi.uminho.pt:21000/api/atlas/types");
            ClientResponse response = webResource.type("application/json").header("Authorization", "Basic " + authStringEnc)
                    .post(ClientResponse.class,
                            type);
            if (response.getStatus() != 201) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + response.getStatus());
            }
            System.out.println("Output from Server .... \n");
            String output = response.getEntity(String.class
            );
            System.out.println(output);
            return output;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

//  Json o Qualified name e que conta , se for igual ele vai atualizar os que sao iguais ao qualified name
}
