/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.sqlparser.v3;

import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author Sadroddin/Sanjit/Dinesh
 */
public class SyncMetadataJsonFileDesign {

    public static StringBuilder log = new StringBuilder();

    public static ArrayList<MappingSpecificationRow> setMetaDataSpec(String json, String ssisdatabaseName, String ssisserverName, String jsonFilePath,
            String defSysName, String defEnvName, HashMap cacheMap, HashMap<String, String> allTablesMap, HashMap<String, String> allDBMap, String defSchema, String filePath) {
        ArrayList<MappingSpecificationRow> finalMapSPecsLists = null;
        try {
            ssisdatabaseName = ssisdatabaseName.toUpperCase();
            ssisserverName = ssisserverName.toUpperCase();
            ssisdatabaseName = ssisdatabaseName.trim();
            ssisserverName = ssisserverName.trim();
            ObjectMapper mapper = new ObjectMapper();
            finalMapSPecsLists = new ArrayList();
            Set<String> removeDuplicate = new HashSet();
            json = json.replace(",\"childNodes\":[]", "");
            Mapping mapObj = mapper.readValue(json, Mapping.class);

            ArrayList<MappingSpecificationRow> mapSPecsLists = (ArrayList<MappingSpecificationRow>) mapObj.getMappingSpecifications();
            String mapName = mapObj.getMappingName();
            for (MappingSpecificationRow mapSPecRow : mapSPecsLists) {
                try {

                    String sourcetableName = mapSPecRow.getSourceTableName();
                    String targetTableName = mapSPecRow.getTargetTableName();

                    String querySourceServerName = "";
                    String querySourceDatabaseName = "";
                    String querySourceSchemaName = "";
                    String querySourceTableName = "";

                    String queryTargetServerName = "";
                    String queryTargetDatabaseName = "";
                    String queryTargetSchemaName = "";
                    String queryTargetTableName = "";

//                try {
                    ArrayList<String> sourceSystemList = new ArrayList();
                    ArrayList<String> sourceEnvironmentList = new ArrayList();
                    ArrayList<String> sourceTableList = new ArrayList();
                    ArrayList<String> targetSystemList = new ArrayList();
                    ArrayList<String> targetEnvironmentList = new ArrayList();
                    ArrayList<String> targetTableList = new ArrayList();
                    if (!StringUtils.isBlank(sourcetableName)) {
                        ArrayList<String> sourceDetailedTableNameList = getTableName(sourcetableName, defSchema);
                        for (String sourceDetailedTableName : sourceDetailedTableNameList) {
                            try {
                                querySourceServerName = sourceDetailedTableName.split("erwinseprator")[0];
                                querySourceDatabaseName = sourceDetailedTableName.split("erwinseprator")[1];
                                querySourceSchemaName = sourceDetailedTableName.split("erwinseprator")[2];
                                querySourceTableName = sourceDetailedTableName.split("erwinseprator")[3];

                                if (querySourceDatabaseName.equals("")) {
                                    String useLineDataBaseName = "";
                                    if (ssisdatabaseName.contains("separatorUseLine")) {

                                        try {
                                            useLineDataBaseName = ssisdatabaseName.split("separatorUseLine")[1];
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        if (!StringUtils.isBlank(useLineDataBaseName)) {
                                            querySourceDatabaseName = useLineDataBaseName;
                                        }
                                    } else {
                                        querySourceDatabaseName = ssisdatabaseName;
                                    }

                                }
                                if (querySourceServerName.equals("")) {
                                    querySourceServerName = ssisserverName;
                                }
                                String sourcesystemEnvironment = newmetasync(querySourceTableName.trim().toUpperCase(), querySourceDatabaseName, querySourceServerName, jsonFilePath, defSysName, defEnvName, cacheMap, allTablesMap, allDBMap, mapName, querySourceSchemaName, defSchema);
                                sourceSystemList.add(sourcesystemEnvironment.split("##")[0]);
                                sourceEnvironmentList.add(sourcesystemEnvironment.split("##")[1]);
                                sourceTableList.add(sourcesystemEnvironment.split("##")[2]);
                            } catch (Exception e) {

                            }
                        }
                    }
                    if (!StringUtils.isBlank(targetTableName)) {
                        ArrayList<String> targetDetailedTableNameList = getTableName(targetTableName, defSchema);
                        for (String targetDetailedTableName : targetDetailedTableNameList) {
                            try {
                                queryTargetServerName = targetDetailedTableName.split("erwinseprator")[0];
                                queryTargetDatabaseName = targetDetailedTableName.split("erwinseprator")[1];
                                queryTargetSchemaName = targetDetailedTableName.split("erwinseprator")[2];
                                queryTargetTableName = targetDetailedTableName.split("erwinseprator")[3];

                                if (queryTargetDatabaseName.equals("")) {
                                    String useLineDataBaseName = "";
                                    if (ssisdatabaseName.contains("separatorUseLine")) {
                                        try {
                                            useLineDataBaseName = ssisdatabaseName.split("separatorUseLine")[1];
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }

                                        if (!StringUtils.isBlank(useLineDataBaseName)) {
                                            queryTargetDatabaseName = useLineDataBaseName;
                                        }
                                    } else {
                                        queryTargetDatabaseName = ssisdatabaseName;
                                    }

                                }

                                if (queryTargetServerName.equals("")) {
                                    queryTargetServerName = ssisserverName;
                                }
                                String targetsystemEnvironment = newmetasync(queryTargetTableName.trim().toUpperCase(), queryTargetDatabaseName, queryTargetServerName, jsonFilePath, defSysName, defEnvName, cacheMap, allTablesMap, allDBMap, mapName, queryTargetSchemaName, defSchema);
                                targetSystemList.add(targetsystemEnvironment.split("##")[0]);
                                targetEnvironmentList.add(targetsystemEnvironment.split("##")[1]);
                                targetTableList.add(targetsystemEnvironment.split("##")[2]);
                            } catch (Exception e) {
                            }
                        }
                    }

                    mapSPecRow.setSourceTableName(StringUtils.join(sourceTableList, "\n"));
                    mapSPecRow.setSourceSystemName(StringUtils.join(sourceSystemList, "\n"));
                    mapSPecRow.setSourceSystemEnvironmentName(StringUtils.join(sourceEnvironmentList, "\n"));

                    mapSPecRow.setTargetTableName(StringUtils.join(targetTableList, "\n"));
                    mapSPecRow.setTargetSystemName(StringUtils.join(targetSystemList, "\n"));
                    mapSPecRow.setTargetSystemEnvironmentName(StringUtils.join(targetEnvironmentList, "\n"));

                    removeDublicate(mapSPecRow, finalMapSPecsLists, removeDuplicate);
                } catch (Exception e) {

                }
            }

//            if (!StringUtils.isBlank(filePath)) {
//                File logFileDir = new File(filePath);
//                if (logFileDir.exists()) {
//                    writeUnsyncTableDataToFile(filePath);
//                } else {
//                    logFileDir.mkdirs();
//                    writeUnsyncTableDataToFile(filePath);
//                }
//            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
//        return mapjson;
        return finalMapSPecsLists;
    }

    public static ArrayList<String> getTableName(String inputTableName, String defaultSchema) {
        String serverName = "";
        String databaseName = "";
        String schemaName = "";
        String tableName = "";
        String returnValue = "";
        ArrayList<String> returnValueList = new ArrayList();
        try {
            String[] inputTableNameArr = inputTableName.split("\n");
            for (int i = 0; i < inputTableNameArr.length; i++) {
                serverName = "";
                databaseName = "";
                schemaName = "";
                tableName = "";
                returnValue = "";
                inputTableName = inputTableNameArr[i];
                if (inputTableName.contains("].[")) {
                    String[] tableStringArry = inputTableName.split("\\]\\.\\[");
                    if (tableStringArry.length == 4) {
                        serverName = tableStringArry[0];
                        databaseName = tableStringArry[1];
                        schemaName = tableStringArry[2];
                        tableName = tableStringArry[3];
                    } else if (tableStringArry.length == 3) {
                        databaseName = tableStringArry[0];
                        schemaName = tableStringArry[1];
                        tableName = tableStringArry[2];
                        try {
                            if (tableName.contains("].")) {
                                serverName = databaseName;
                                databaseName = schemaName;
                                schemaName = tableName.split("\\]\\.")[0];
                                tableName = tableName.split("\\]\\.")[1];

                            }
                        } catch (Exception e) {

                        }
                    } else if (tableStringArry.length == 2) {
                        schemaName = tableStringArry[0];
                        tableName = tableStringArry[1];
                        try {
                            if (tableName.contains("].")) {
                                databaseName = schemaName;
                                schemaName = tableName.split("\\]\\.")[0];
                                tableName = tableName.split("\\]\\.")[1];
                            }
                        } catch (Exception e) {

                        }
                    }
                } else if (inputTableName.contains("[") && inputTableName.contains("]") && inputTableName.contains(".")) {
                    tableName = inputTableName;
                    try {
                        if (tableName.contains("].")) {
                            schemaName = tableName.split("\\]\\.")[0];
                            tableName = tableName.split("\\]\\.")[1];
                        }
                    } catch (Exception e) {

                    }
                } else {

                    String[] tableStringArry = inputTableName.split("\\.");
                    if (tableStringArry.length == 4) {
                        serverName = tableStringArry[0];
                        databaseName = tableStringArry[1];
                        schemaName = tableStringArry[2];
                        if (StringUtils.isBlank(schemaName)) {
                            schemaName = defaultSchema;
                        }
                        tableName = tableStringArry[3];
                    } else if (tableStringArry.length == 3) {
                        databaseName = tableStringArry[0];
                        schemaName = tableStringArry[1];
                        if (StringUtils.isBlank(schemaName)) {
                            schemaName = defaultSchema;
                        }
                        tableName = tableStringArry[2];
                    } else if (tableStringArry.length == 2) {
                        schemaName = tableStringArry[0];
                        tableName = tableStringArry[1];
                    } else {
                        tableName = inputTableName;
                    }
                }

                if (!"".equals(tableName.trim()) && !StringUtils.isBlank(schemaName)) {
                    returnValue = serverName + "erwinseprator" + databaseName + "erwinseprator" + schemaName + "erwinseprator" + schemaName + "." + tableName;
                } else {
                    returnValue = serverName + "erwinseprator" + databaseName + "erwinseprator" + schemaName + "erwinseprator" + tableName;
                }

                returnValue = returnValue.replace("[", "").replace("]", "");
                returnValueList.add(returnValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnValueList;
    }

    public static String newmetasync(String tableName, String ssisdatabaseName, String ssisserverName, String jsonFileDir,
            String defSysName, String defEnvName, HashMap cacheMap, HashMap<String, String> allTablesMap, HashMap<String, String> allDBMap, String mapName, String querySchemaName, String defaultSchema) {

        ssisdatabaseName = ssisdatabaseName.toUpperCase().replaceAll("[^a-zA-Z0-9]", "_");
        ssisserverName = ssisserverName.toUpperCase().replaceAll("[^a-zA-Z0-9]", "_");
        String systemName = "";
        String environementName = "";
        String schemaName = "";
        JSONParser parser = new JSONParser();
        try {
            ObjectMapper mapper = new ObjectMapper();

            if (ssisserverName == null || ssisserverName.equals("") && !StringUtils.isBlank(ssisdatabaseName)) {

                ssisserverName = allDBMap.get(ssisdatabaseName.toUpperCase());
            }
//            String jsonFilePath = jsonFileDir + ssisserverName + "_" + ssisdatabaseName + ".json";
            String jsonFilePath = "";
            if (!StringUtils.isBlank(ssisserverName)) {
                jsonFilePath = jsonFileDir + ssisserverName + "_" + ssisdatabaseName + ".json";
            } else {
                jsonFilePath = jsonFileDir + ssisdatabaseName + ".json";
            }

            File jsonFile = new File(jsonFilePath);

            String sourceSysEnvInfo = allTablesMap.get(tableName);
            if (sourceSysEnvInfo != null && !StringUtils.isBlank(jsonFileDir.trim()) && new File(jsonFileDir).exists()) {
                if (!sourceSysEnvInfo.contains("@ERWIN@")) {
                    String[] sourceSysEnvInfoArr = sourceSysEnvInfo.split("#");
                    if (sourceSysEnvInfoArr.length == 3) {
                        systemName = sourceSysEnvInfoArr[1];
                        environementName = sourceSysEnvInfoArr[0];
                        schemaName = sourceSysEnvInfoArr[2];
                        tableName = schemaName + "." + tableName;
                    } else if (sourceSysEnvInfoArr.length == 2) {
                        systemName = sourceSysEnvInfoArr[1];
                        environementName = sourceSysEnvInfoArr[0];
                    }
                } else if (!jsonFile.exists()) {
                    if (sourceSysEnvInfo.contains("@ERWIN@")) {
                        sourceSysEnvInfo = sourceSysEnvInfo.split("@ERWIN@")[0];
                    }
                    String[] sourceSysEnvInfoArr = sourceSysEnvInfo.split("#");
                    if (sourceSysEnvInfoArr.length == 3) {
                        systemName = sourceSysEnvInfoArr[1];
                        environementName = sourceSysEnvInfoArr[0];
                        schemaName = sourceSysEnvInfoArr[2];
                        tableName = schemaName + "." + tableName;
                    } else if (sourceSysEnvInfoArr.length == 2) {
                        systemName = sourceSysEnvInfoArr[1];
                        environementName = sourceSysEnvInfoArr[0];
                    }
                } else {
                    HashMap serverDatabaseMap = (HashMap) cacheMap.get(ssisserverName + "_" + ssisdatabaseName);
                    if (serverDatabaseMap != null) {
                        List tables = (ArrayList) serverDatabaseMap.get("Tables");
                        List schemaList = (ArrayList) serverDatabaseMap.get("Schemas");
//                        if (tables.contains(tableName)) {
                        if (tables.toString().toLowerCase().contains(tableName.toLowerCase())) {
                            systemName = serverDatabaseMap.get("SystemName").toString();
                            environementName = serverDatabaseMap.get("EnvironmentName").toString();
                            if (StringUtils.isBlank(querySchemaName) && schemaList.size() > 0) {
                                tableName = schemaList.get(0) + "." + tableName;
                            }
                        } else {
                            systemName = defSysName;
                            environementName = defEnvName;

                            log.append("MapName = " + mapName + "\n" + "TableName = " + tableName + "\n");
                        }

                    } else {
                        FileReader fileReader = null;
                        try {
                            fileReader = new FileReader(jsonFile);
                            Object obj = parser.parse(fileReader);
                            JSONObject jsonObject = (JSONObject) obj;
                            serverDatabaseMap = mapper.convertValue(jsonObject, HashMap.class);
                            cacheMap.put(ssisserverName + "_" + ssisdatabaseName, serverDatabaseMap);
//                            Set tables = (HashSet) serverDatabaseMap.get("Tables");
                            List tables = (ArrayList) serverDatabaseMap.get("Tables");
                            List schemaList = (ArrayList) serverDatabaseMap.get("Schemas");
//                            if (tables.contains(tableName)) {
                            if (tables.toString().toLowerCase().contains(tableName.toLowerCase())) {
                                systemName = serverDatabaseMap.get("SystemName").toString();
                                environementName = serverDatabaseMap.get("EnvironmentName").toString();
                                if (StringUtils.isBlank(querySchemaName) && schemaList.size() > 0) {
                                    tableName = schemaList.get(0) + "." + tableName;
                                }
                            } else {
                                systemName = defSysName;
                                environementName = defEnvName;

                                log.append("MapName = " + mapName + "\n" + "TableName = " + tableName + "\n");
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            systemName = defSysName;
                            environementName = defEnvName;

                            log.append("MapName = " + mapName + "\n" + "TableName = " + tableName + "\n");
                        } finally {
                            try {
                                if (fileReader != null) {
                                    fileReader.close();
                                }
                            } catch (Exception e) {

                            }
                        }
                    }
                }
            } else {
                systemName = defSysName;
                environementName = defEnvName;

                log.append("MapName = " + mapName + "\n" + "TableName = " + tableName + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        tableName = getTableNameDefaults(tableName, systemName, environementName, defSysName, defEnvName, querySchemaName, defaultSchema);
        return systemName + "##" + environementName + "##" + tableName;
    }

    public static String getTableNameDefaults(String inputTableName, String systemName, String environementName, String defSysName, String defEnvName, String querySchemaName, String defaultSchemaName) {
        try {
            if (inputTableName.contains("RESULT_OF_") || inputTableName.contains("INSERT-SELECT") || inputTableName.contains("UPDATE-") || inputTableName.contains("RS") || inputTableName.contains("MERGE") || inputTableName.contains("UNION")) {
                return inputTableName;
            } else if (systemName.equals(defSysName) && environementName.equals(defEnvName) && StringUtils.isBlank(querySchemaName) && !StringUtils.isBlank(defaultSchemaName)) {
                inputTableName = defaultSchemaName + "." + inputTableName;
            }
        } catch (Exception e) {

        }
        return inputTableName;
    }

    public static void writeUnsyncTableDataToFile(String filePath) {

        System.out.println("filePath---" + filePath);
        FileWriter writer = null;
        String fileDate = "";
        try {
//            writer = new FileWriter(filePath + "/" + "Failed_Syncuplogs" + ".txt");
            fileDate = new SimpleDateFormat("yyyyMMdd").format(new Date());
            writer = new FileWriter(filePath + "/" + "Failed_Syncuplogs" + "_" + fileDate + ".txt", true);
            writer.write(log.toString());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static HashMap<String, String> allTablesMap(String jsonFilePath) {

        HashMap<String, String> allTablesMap = new HashMap<>();
        FileReader fileReader = null;
        try {
            if (!new File(jsonFilePath).exists()) {
                return allTablesMap;
            }
            JSONParser parser = new JSONParser();
            fileReader = new FileReader(jsonFilePath + "AllTables.json");
            Object obj = parser.parse(fileReader);

            JSONObject jsonObject = (JSONObject) obj;

            ObjectMapper mapper = new ObjectMapper();
            allTablesMap = mapper.convertValue(jsonObject.get("Tables"), HashMap.class);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fileReader != null) {
                    fileReader.close();
                }
            } catch (Exception e) {

            }
        }
        return allTablesMap;
    }

    public static HashMap<String, String> allDBMap(String jsonFilePath) {

        HashMap<String, String> allDBMap = new HashMap<>();
        FileReader fileReader = null;
        try {
            if (!new File(jsonFilePath).exists()) {
                return allDBMap;
            }
            JSONParser parser = new JSONParser();
            fileReader = new FileReader(jsonFilePath + "AllTables.json");
            Object obj = parser.parse(fileReader);

            JSONObject jsonObject = (JSONObject) obj;

            ObjectMapper mapper = new ObjectMapper();
            allDBMap = mapper.convertValue(jsonObject.get("Databases"), HashMap.class);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fileReader != null) {
                    fileReader.close();
                }
            } catch (Exception e) {

            }
        }
        return allDBMap;
    }

    public static void removeDublicate(MappingSpecificationRow mapSPecRow, ArrayList<MappingSpecificationRow> finalMapSPecsLists, Set<String> removeDuplicate) {
        String sourceTableName = mapSPecRow.getSourceTableName().trim();
        String sourceColumnName = mapSPecRow.getSourceColumnName().trim();
        String targetTableName = mapSPecRow.getTargetTableName().trim();
        String targetColumnName = mapSPecRow.getTargetColumnName().trim();
        String businessRule = mapSPecRow.getBusinessRule();
        if ("".equals(sourceTableName) && !"".equals(businessRule)) {
            sourceTableName = targetTableName;
        } else if (sourceTableName.contains(targetTableName)) {
            targetTableName = "";
        }
        String stringSpecRow = sourceTableName + "#" + sourceColumnName + "#" + targetTableName + "#" + targetColumnName + "#" + businessRule;
        if (!removeDuplicate.contains(stringSpecRow)) {
            finalMapSPecsLists.add(mapSPecRow);
            removeDuplicate.add(stringSpecRow);
        }

    }
}
