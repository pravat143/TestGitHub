/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.metadata_zovio_af;

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
public class SyncMetadataJFDServerDBSchema {

    public static StringBuilder log = new StringBuilder();
    public static String DELIMITER = "@erwin@";
    public static Set<String> intermediateComponents = new HashSet();

    public static ArrayList<MappingSpecificationRow> setMetaDataSpec(String json,
            String ssisdatabaseName, String ssisserverName, String jsonFilePath,
            String defSysName, String defEnvName, HashMap cacheMap,
            HashMap<String, String> allDBMap,
            String defSchema, String delimiter, Set<String> intermediateCompSet) {
        DELIMITER = delimiter;
        intermediateComponents = intermediateCompSet;
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
                                querySourceServerName = sourceDetailedTableName.split(DELIMITER)[0];//FIX T5 Adding Delimiter
                                querySourceDatabaseName = sourceDetailedTableName.split(DELIMITER)[1];//FIX T5 Adding Delimiter
                                querySourceSchemaName = sourceDetailedTableName.split(DELIMITER)[2];//FIX T5 Adding Delimiter
                                querySourceTableName = sourceDetailedTableName.split(DELIMITER)[3];//FIX T5 Adding Delimiter

                                if (querySourceDatabaseName.equals("")) {
                                    String useLineDataBaseName = "";
                                    if (ssisdatabaseName.contains(DELIMITER)) {

                                        try {
                                            useLineDataBaseName = ssisdatabaseName.split(DELIMITER)[1];
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
                                String sourcesystemEnvironment = newmetasync(querySourceTableName.trim().toUpperCase(), querySourceDatabaseName, querySourceServerName, jsonFilePath, defSysName, defEnvName, cacheMap, allDBMap, mapName, querySourceSchemaName, defSchema, DELIMITER);
                                sourceSystemList.add(sourcesystemEnvironment.split(DELIMITER)[0]);//FIX T5 Adding Delimiter
                                sourceEnvironmentList.add(sourcesystemEnvironment.split(DELIMITER)[1]);//FIX T5 Adding Delimiter
                                sourceTableList.add(sourcesystemEnvironment.split(DELIMITER)[2]);//FIX T5 Adding Delimiter
                            } catch (Exception e) {

                            }
                        }
                    }
                    if (!StringUtils.isBlank(targetTableName)) {
                        ArrayList<String> targetDetailedTableNameList = getTableName(targetTableName, defSchema);
                        for (String targetDetailedTableName : targetDetailedTableNameList) {
                            try {
                                queryTargetServerName = targetDetailedTableName.split(DELIMITER)[0];//FIX T5 Adding Delimiter
                                queryTargetDatabaseName = targetDetailedTableName.split(DELIMITER)[1];//FIX T5 Adding Delimiter
                                queryTargetSchemaName = targetDetailedTableName.split(DELIMITER)[2];//FIX T5 Adding Delimiter
                                queryTargetTableName = targetDetailedTableName.split(DELIMITER)[3];//FIX T5 Adding Delimiter

                                if (queryTargetDatabaseName.equals("")) {
                                    String useLineDataBaseName = "";
                                    if (ssisdatabaseName.contains(DELIMITER)) {
                                        try {
                                            useLineDataBaseName = ssisdatabaseName.split(DELIMITER)[1];
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
                                String targetsystemEnvironment = newmetasync(queryTargetTableName.trim().toUpperCase(), queryTargetDatabaseName, queryTargetServerName, jsonFilePath, defSysName, defEnvName, cacheMap, allDBMap, mapName, queryTargetSchemaName, defSchema, DELIMITER);
                                targetSystemList.add(targetsystemEnvironment.split(DELIMITER)[0]);//FIX T5 Adding Delimiter
                                targetEnvironmentList.add(targetsystemEnvironment.split(DELIMITER)[1]);//FIX T5 Adding Delimiter
                                targetTableList.add(targetsystemEnvironment.split(DELIMITER)[2]);//FIX T5 Adding Delimiter
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

        } catch (Exception ex) {
            ex.printStackTrace();
        }

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
                ArrayList<String> tablePartsList = getTablePartsList(inputTableName);
                if (tablePartsList.size() >= 4) {
                    serverName = tablePartsList.get(0);
                    databaseName = tablePartsList.get(1);
                    schemaName = tablePartsList.get(2);
                    tableName = tablePartsList.get(3);
                } else if (tablePartsList.size() >= 3) {

                    databaseName = tablePartsList.get(0);
                    schemaName = tablePartsList.get(1);
                    tableName = tablePartsList.get(2);
                } else if (tablePartsList.size() >= 2) {

                    schemaName = tablePartsList.get(0);
                    tableName = tablePartsList.get(1);
                } else if (tablePartsList.size() >= 1) {

                    tableName = tablePartsList.get(0);
                }

                if (!"".equals(tableName.trim()) && !StringUtils.isBlank(schemaName)) {
                    returnValue = serverName + DELIMITER + databaseName + DELIMITER + schemaName + DELIMITER + schemaName + "." + tableName;//FIX T5 Adding Delimiter
                } else {
                    returnValue = serverName + DELIMITER + databaseName + DELIMITER + schemaName + DELIMITER + tableName;//FIX T5 Adding Delimiter
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
            String defSysName, String defEnvName, HashMap cacheMap, HashMap<String, String> allDBMap, String mapName, String querySchemaName, String defaultSchema, String delimiter) {

        DELIMITER = delimiter;
        ssisdatabaseName = ssisdatabaseName.toUpperCase().replaceAll("[^a-zA-Z0-9]", "_");
        ssisserverName = ssisserverName.toUpperCase().replaceAll("[^a-zA-Z0-9]", "_");
        String systemName = "";
        String environementName = "";
        String schemaName = "";
        tableName = tableName.toUpperCase();

        if (tableName.equalsIgnoreCase("dbo.Agent_State_Details_ITS")) {
            int temp = 0;
        }
        JSONParser parser = new JSONParser();
        try {
            ObjectMapper mapper = new ObjectMapper();

            String jsonFilePath = "";
            if (!StringUtils.isBlank(ssisserverName)) {
                jsonFilePath = jsonFileDir + ssisserverName + "_" + ssisdatabaseName + ".json";
            } else {
                jsonFilePath = jsonFileDir + ssisdatabaseName + ".json";
            }

            if (ssisdatabaseName.equalsIgnoreCase(defEnvName) && ssisserverName.equalsIgnoreCase(defSysName)) {
                return defSysName + DELIMITER + defEnvName + DELIMITER + tableName;//FIX T5 Adding Delimiter  
            }
            File jsonFile = new File(jsonFilePath);

            if ((StringUtils.isBlank(ssisserverName) && !StringUtils.isBlank(ssisdatabaseName)) || !jsonFile.exists()) {

                ssisserverName = allDBMap.get(ssisdatabaseName.toUpperCase());
                jsonFilePath = "";
                if (!StringUtils.isBlank(ssisserverName)) {
                    jsonFilePath = jsonFileDir + ssisserverName + "_" + ssisdatabaseName + ".json";
                } else {
                    jsonFilePath = jsonFileDir + ssisdatabaseName + ".json";
                }

                jsonFile = new File(jsonFilePath);

            }
            if (!StringUtils.isBlank(jsonFileDir.trim()) && new File(jsonFileDir).exists()) {
                HashMap serverDatabaseMap = (HashMap) cacheMap.get(ssisserverName + "_" + ssisdatabaseName);
                if (serverDatabaseMap != null) {
                    List tables = (ArrayList) serverDatabaseMap.get("Tables");
                    List schemaList = (ArrayList) serverDatabaseMap.get("Schemas");
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

                } else if (jsonFile.exists()) {
                    FileReader fileReader = null;
                    try {
                        fileReader = new FileReader(jsonFile);
                        Object obj = parser.parse(fileReader);
                        JSONObject jsonObject = (JSONObject) obj;
                        serverDatabaseMap = mapper.convertValue(jsonObject, HashMap.class);
                        cacheMap.put(ssisserverName + "_" + ssisdatabaseName, serverDatabaseMap);
                        List tables = (ArrayList) serverDatabaseMap.get("Tables");
                        List schemaList = (ArrayList) serverDatabaseMap.get("Schemas");
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
                } else {
                    systemName = defSysName;
                    environementName = defEnvName;
                    log.append("MapName = " + mapName + "\n" + "TableName = " + tableName + "\n");
                }
            } else {
                systemName = defSysName;
                environementName = defEnvName;
                log.append("MapName = " + mapName + "\n" + "TableName = " + tableName + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        tableName = getTableNameDefaults(tableName, systemName, environementName, defSysName,
                defEnvName, querySchemaName, defaultSchema);
        return systemName + DELIMITER + environementName + DELIMITER + tableName;//FIX T5 Adding Delimiter
    }

    public static String getTableNameDefaults(String inputTableName, String systemName,
            String environementName, String defSysName, String defEnvName, String querySchemaName,
            String defaultSchemaName) {
        try {// FIX S1 for finding intermediate TableName and appending Schema Name
//            if (MappingCreator.intermediateComponents != null
//                    && MappingCreator.intermediateComponents.contains(inputTableName)) {
            if (intermediateComponents != null
                    && intermediateComponents.contains(inputTableName)) {
                return inputTableName;
            } else if (systemName.equals(defSysName) && environementName.equals(defEnvName)
                    && StringUtils.isBlank(querySchemaName) && !StringUtils.isBlank(defaultSchemaName)) {
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
// FIX S2 Newly added common Method for preparing Tables and databases Map

    public static HashMap<String, String> getMap(String jsonFilePath, String type) {

        HashMap<String, String> map = new HashMap<>();
        FileReader fileReader = null;
        try {
            if (!new File(jsonFilePath).exists()) {
                return map;
            }
            JSONParser parser = new JSONParser();
            fileReader = new FileReader(jsonFilePath + "AllTables.json");
            Object obj = parser.parse(fileReader);

            JSONObject jsonObject = (JSONObject) obj;

            ObjectMapper mapper = new ObjectMapper();
            map
                    = mapper.convertValue(jsonObject.get(type), HashMap.class
                    );
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
        return map;
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

    public static ArrayList<String> getTablePartsList(String inputTableName) {
        ArrayList<String> list = new ArrayList();
        if (inputTableName.contains("[") || inputTableName.contains("]")) {
            try {
                while (true) {
                    if (inputTableName.equals("")) {
                        break;
                    }
                    if (inputTableName.trim().startsWith("[")) {
                        list.add(inputTableName.substring(inputTableName.indexOf("[") + 1, inputTableName.indexOf("]")).trim());
                        if (inputTableName.contains("].")) {
                            inputTableName = inputTableName.substring(inputTableName.indexOf("].") + 2);
                        } else {
                            inputTableName = "";
                        }

                    } else if (inputTableName.contains(".")) {
                        list.add(inputTableName.substring(0, inputTableName.indexOf(".")).trim());
                        inputTableName = inputTableName.substring(inputTableName.indexOf(".") + 1);
                    } else {
                        list.add(inputTableName.trim());
                        inputTableName = "";
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        } else if (inputTableName.contains("\"")) {
            try {
                while (true) {
                    if (inputTableName.equals("")) {
                        break;
                    }
                    if (inputTableName.trim().startsWith("\"")) {
                        list.add(inputTableName.substring(inputTableName.indexOf("\"") + 1, inputTableName.indexOf("\"", inputTableName.indexOf("\"") + 1)).trim());
                        if (inputTableName.contains("\".")) {
                            inputTableName = inputTableName.substring(inputTableName.indexOf("\".") + 2);
                        } else {
                            inputTableName = "";
                        }

                    } else if (inputTableName.contains(".")) {
                        list.add(inputTableName.substring(0, inputTableName.indexOf(".")).trim());
                        inputTableName = inputTableName.substring(inputTableName.indexOf(".") + 1);
                    } else {
                        list.add(inputTableName.trim());
                        inputTableName = "";
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        } else if (inputTableName.contains(".")) {

            String dotArray[] = inputTableName.split("\\.");
            for (String subTable : dotArray) {
                list.add(subTable);
            }

        } else {
            list.add(inputTableName.trim());
        }
        return list;
    }

}
