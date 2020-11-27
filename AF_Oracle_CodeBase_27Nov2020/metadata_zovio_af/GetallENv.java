/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.metadata_zovio_af;

import com.ads.api.beans.sm.SMEnvironment;
import com.ads.api.beans.sm.SMSystem;
import com.ads.api.beans.sm.SMTable;
import com.ads.api.util.SystemManagerUtil;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

/**
 *
 * @author SanjitSourav
 */
public class GetallENv {

    public static HashMap<String, String> allTables = null;
    public static HashMap<String, String> allDb = null;
    public static String executionDateForFile = null;
    public  static String metadataFilePath="";
    public static String allEnvironments(SystemManagerUtil systemManagerUtil,
            String jsonPath,
            String jsonFolder       
    ) {
        allTables = new HashMap<>();
        allDb = new HashMap<>();
        try {
            if (StringUtils.isBlank(jsonPath)) {
                return "Kindly Specify a MetaData_Directory.";
            }
//            long executionTime = System.currentTimeMillis();
//            Calendar cal = Calendar.getInstance();
//            cal.setTimeInMillis(executionTime);
//            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
//            executionDateForFile = df.format(cal.getTime());
//            writeLogDataIntoFile(logFilePath, logFileName + "_" + executionDateForFile, logHeaderWithCATParam);

            String erwinName = "";
            String metadataJsonDir = "";
            try {
                if (jsonPath.contains("\\")) {
                    jsonPath = jsonPath.replaceAll("\\\\", "/");
                } else {
                    jsonPath = jsonPath;
                }
                if (!StringUtils.isBlank(jsonFolder)) {
                    if (jsonFolder.contains("\\")) {
                        jsonFolder = jsonFolder.replaceAll("\\\\", "/");
                    } else {
                        jsonFolder = jsonFolder;
                    }

                    if (jsonFolder.split("/").length == 3) {
                        erwinName = jsonFolder.split("/")[1];
                        metadataJsonDir = jsonFolder.split("/")[2];
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (jsonPath.substring(jsonPath.lastIndexOf("/") + 1).equals("")) {
                jsonPath = jsonPath.substring(0, jsonPath.length() - 1);
            }
            if (jsonPath.substring(jsonPath.lastIndexOf("/") + 1).toLowerCase().contains(erwinName.toLowerCase())) {

                if (!jsonPath.toLowerCase().contains(metadataJsonDir.toLowerCase())) {
                    jsonPath = jsonPath + "/" + metadataJsonDir;
                }

            } else if (jsonPath.substring(jsonPath.lastIndexOf("/") + 1).toLowerCase().contains(metadataJsonDir.toLowerCase())) {

                jsonPath = jsonPath;
            } else {
                jsonPath = jsonPath + "/" + jsonFolder;
            }
            String lastChar = jsonPath.substring(jsonPath.length() - 1);
            if (!lastChar.equals("/")) {
                jsonPath += "/";
            }
            File directory = new File(jsonPath);
            if (!directory.exists()) {
                directory.mkdirs();
            }
//        FileUtils.cleanDirectory(directory);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ArrayList<SMSystem> listOfSystems = null;

        try {
            listOfSystems = systemManagerUtil.getSystems();
            for (int m = 0; m < listOfSystems.size();
                    m++) {
                SMSystem Sytems = listOfSystems.get(m);
                int SystemId = Sytems.getSystemId();
                String systemName = Sytems.getSystemName();

                ArrayList<SMEnvironment> listofEnvironments = systemManagerUtil.getEnvironments(SystemId);

                for (int n = 0; n < listofEnvironments.size();
                        n++) {
                    Set<String> tableSchemaSet = new HashSet<>();
                    Set<String> schemaSet = new HashSet<>();
                    JSONObject jsonObject = new JSONObject();
                    SMEnvironment smEnvironment = listofEnvironments.get(n);
                    int EnvironmentId = smEnvironment.getEnvironmentId();
                    String serverName = smEnvironment.getDatabaseIPAddress();
                    String environmentName = smEnvironment.getSystemEnvironmentName();
                    String dataBaseName = smEnvironment.getDatabaseName();
                    jsonObject.put("SystemName", systemName);
                    jsonObject.put("EnvironmentName", environmentName);
                    ArrayList<SMTable> tablesEnvlist = systemManagerUtil.getEnvironmentTables(EnvironmentId);

                    for (int i = 0; i < tablesEnvlist.size();
                            i++) {
                        SMTable tableobj = tablesEnvlist.get(i);
                        String tableName = tableobj.getTableName();
                        if (tableName.contains(".")) {
                            String schemaName = "";

                            try {
                                String tableNameWithOutSchema = "";
                                schemaName = tableName.split("\\.")[0].trim();
                                tableSchemaSet.add(tableName);
                                tableNameWithOutSchema = tableName.substring(tableName.indexOf(".") + 1, tableName.length());
                                tableSchemaSet.add(tableNameWithOutSchema);
                                schemaSet.add(schemaName);
                                String key1 = tableNameWithOutSchema.toUpperCase();
                                String key2 = tableName.toUpperCase();
                                String key3 = dataBaseName.replaceAll("[^a-zA-Z0-9]", "_").toUpperCase();
                                String value2 = serverName.replaceAll("[^a-zA-Z0-9]", "_").toUpperCase();
                                String value = environmentName + "#" + systemName + "#" + schemaName;
                                String value3 = environmentName + "#" + systemName;
                                if (allTables.get(key2) != null) {
                                    allTables.put(key2, allTables.get(key2) + "@ERWIN@" + value3);
                                } else {
                                    allTables.put(key2, value3);
                                }
                                if (allTables.get(key1) != null) {
                                    allTables.put(key1, allTables.get(key1) + "@ERWIN@" + value);
                                } else {
                                    allTables.put(key1, value);
                                }
                                allDb.put(key3, value2);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        } else {
                            String tableNameUpperCase = tableName.toUpperCase();
                            String systemEnvironment = environmentName + "#" + systemName;
                            if (allTables.get(tableNameUpperCase) != null) {
                                allTables.put(tableNameUpperCase, allTables.get(tableNameUpperCase) + "@ERWIN@" + systemEnvironment);
                            } else {
                                allTables.put(tableNameUpperCase, systemEnvironment);
                            }
                            tableSchemaSet.add(tableName.toUpperCase());
                        }
                        jsonObject.put("Tables", tableSchemaSet);
                        jsonObject.put("Schemas", schemaSet);
                        FileWriter file = null;
                        try {
//                            FileWriter file = new FileWriter(jsonPath + serverName + "_" + dataBaseName + ".json", false);
                            String fileName = "";
                            if (!StringUtils.isBlank(serverName)) {
                                fileName = serverName.replaceAll("[^a-zA-Z0-9]", "_").toUpperCase() + "__" + dataBaseName.replaceAll("[^a-zA-Z0-9]", "_").toUpperCase();
                            } else {
                                fileName = tableName.toUpperCase();
                            }
//                            file = new FileWriter(jsonPath + serverName.replaceAll("[^a-zA-Z0-9]", "_").toUpperCase() + "_" + dataBaseName.replaceAll("[^a-zA-Z0-9]", "_").toUpperCase() + ".json", false);
                            file = new FileWriter(jsonPath + fileName + ".json", false);
                            metadataFilePath=jsonPath;
                            file.write(jsonObject.toString());
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            try {
                                if (file != null) {
                                    file.close();
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        JSONObject jsonObject1 = new JSONObject();
        FileWriter file1 = null;
        try {
            jsonObject1.put("Tables", allTables);
            jsonObject1.put("Databases", allDb);
            file1 = new FileWriter(jsonPath + "AllTables.json", false);
            file1.write(jsonObject1.toString());
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (file1 != null) {
                    file1.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "The Json files of all Environment are created Successfully.";
    }

    /**
     *
     * @param logFilePath
     * @param logFileName
     * @param logHeaderWithCATParam
     */
    /*
    public static void writeLogDataIntoFile(String logFilePath,
            String logFileName,
            String logHeaderWithCATParam
    ) {
        BufferedWriter bufferedWriter = null;
        FileWriter fileWriter = null;
        File file = null;
        try {
            File logfilepath = new File(logFilePath);
            if (!logfilepath.exists()) {
                logfilepath.mkdirs();
            }

            String logOutputFilePath = logFilePath + "/" + logFileName + ".log";
            file = new File(logOutputFilePath);
            fileWriter = new FileWriter(file, true);
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(logHeaderWithCATParam);
            bufferedWriter.flush();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (bufferedWriter != null) {
                    bufferedWriter.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (fileWriter != null) {
                    fileWriter.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
*/
    public static String retMetadataFilepth(){
        return metadataFilePath;
    }
    }

