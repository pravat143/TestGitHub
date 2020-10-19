/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.sqlparser.v3;

import com.ads.api.beans.common.Node;
import com.ads.api.beans.kv.KeyValue;
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.mm.Project;
import com.ads.api.beans.mm.Subject;
import com.ads.api.util.KeyValueUtil;
import com.ads.api.util.MappingManagerUtil;
import com.ads.api.util.SystemManagerUtil;
import com.erwin.dataflow.model.xml.dataflow;
import com.erwin.sqlparser.util.CreateMappingVersion;
import com.erwin.sqlparser.wrapper.parser.ErwinSQLWrapper;
import com.erwin.util.XML2Model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.icc.util.RequestStatus;
//import com.icc.util.RequestStatus;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.pp.para.GFmtOpt;
import gudusoft.gsqlparser.pp.para.GFmtOptFactory;
import gudusoft.gsqlparser.pp.stmtformatter.FormatterFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author Balaji / Dinesh Arasankala
 */
public class MappingCreator {

    public static LinkedHashMap<String, HashSet<String>> keyValuesDeailsMap;

    public static LinkedHashMap<String, String> envMapWithTableNameAsKey = null;
    public static LinkedHashMap<String, String> envMapWithDataBaseNameAsKey = null;
    public static String useLine = "";
    public static String DELIMITER = "";
    public static KeyValueUtil kvUtil = null;
    public static int childSubjectId = 0;
    public static ArrayList<String> fileFoldersPathList = null;
    public static HashMap metadatChacheHM = new HashMap();
    public static HashMap<String, String> allTablesMap = new HashMap<String, String>();
    public static HashMap<String, String> allDBMap = new HashMap<String, String>();
    public static String defaultSystemName = "";
    public static String defaultEnviormentName = "";
    public static String defaultSchemaName = "";
    public static StringBuilder exceptionBuilder;
    public static int subjectCatOptionId = 0;

    // Added By Dinesh On June_22_2020
    public String getMappingObjectToJsonForSql(String[] sysenvDetails, HashMap inputpropertiesMap) {
        Mapping mapping = null;
        String mappingJson = "";
        String completeStatus = "";
        String status = "";
        String filePathFromCat = "";
        int projectId = 0;
        MappingManagerUtil mappingManagerUtil = null;
        SystemManagerUtil systemManagerUtil = null;
        String databaseType = "";
        KeyValueUtil keyValueUtil = null;
        String jsonFilePath = "";
        String defaultSchemaNameFromCat = "";
        String loadType = "";
        String executionDateTime = "";
        subjectCatOptionId = 0;

        String logFilePath = "";
        String deleteOrArchiveSourceFile = "";
        String subjectNameFromCat = "";
        String archivePath = "";

        // prepare input data from cat into local varibles
        filePathFromCat = (String) inputpropertiesMap.get("fileDirectory");
        projectId = Integer.parseInt(inputpropertiesMap.get("projectId").toString());
        mappingManagerUtil = (MappingManagerUtil) inputpropertiesMap.get("maputil");
        systemManagerUtil = (SystemManagerUtil) inputpropertiesMap.get("systemManagereUtil");
        databaseType = (String) inputpropertiesMap.get("databaseType");
        keyValueUtil = (KeyValueUtil) inputpropertiesMap.get("keyValueUtil");
        jsonFilePath = (String) inputpropertiesMap.get("jsonPath");
        defaultSchemaNameFromCat = (String) inputpropertiesMap.get("defaultSchemaName");
        loadType = (String) inputpropertiesMap.get("loadTypeFromCat");
        executionDateTime = (String) inputpropertiesMap.get("executionTimeDate");
        logFilePath = (String) inputpropertiesMap.get("syncUpFailedLogs");
        deleteOrArchiveSourceFile = (String) inputpropertiesMap.get("deleteSourceFileType");
        subjectNameFromCat = (String) inputpropertiesMap.get("subjectName");

        if (!StringUtils.isBlank(subjectNameFromCat)) {
            subjectNameFromCat = subjectNameFromCat.replace("\\", "/");

        }

        archivePath = (String) inputpropertiesMap.get("archivePath");
        // completed of preparing input data from the cat

        kvUtil = keyValueUtil;
        fileFoldersPathList = new ArrayList();
        DELIMITER = RelationAnalyzer.DELIMITER;
        int subjectId = 0;
        childSubjectId = 0;
        ObjectMapper objectMapper = new ObjectMapper();

        jsonFilePath = getTheFilePathWithForwardSlash(jsonFilePath);
        archivePath = getTheFilePathWithForwardSlash(archivePath);

        if (logFilePath.contains("\\")) {

            logFilePath = logFilePath.replace("\\", "/");

        }

        allTablesMap = SyncMetadataJsonFileDesign.allTablesMap(jsonFilePath);
        allDBMap = SyncMetadataJsonFileDesign.allDBMap(jsonFilePath);
        metadatChacheHM = new HashMap();
        try {

            File inputFile = null;

            inputFile = new File(filePathFromCat);

            String mapName = "";

            if (!StringUtils.isBlank(subjectNameFromCat)) {

                String[] subjectArraySpilt = null;

                if (subjectNameFromCat.contains(",")) {
                    subjectArraySpilt = subjectNameFromCat.split(",");
                } else {
                    subjectArraySpilt = subjectNameFromCat.split("/");
                }

                int subjectCount = 0;

                if (subjectArraySpilt != null) {
                    for (String subjectHierarchy : subjectArraySpilt) {
                        if (subjectCount == 0 && !StringUtils.isBlank(subjectHierarchy)) {
                            subjectId = createSubject(subjectHierarchy, projectId, mappingManagerUtil);
                        } else {
                            if (!StringUtils.isBlank(subjectHierarchy)) {
                                subjectId = createChildSubject(subjectHierarchy, projectId, subjectId, mappingManagerUtil);
                            }

                        }
                        if (!StringUtils.isBlank(subjectHierarchy)) {
                            subjectCount++;
                        }

                    }
                }

                subjectCatOptionId = subjectId;
            }

            File[] sqlfilearr = inputFile.listFiles();
            for (File sqlFile : sqlfilearr) {

                if ((filePathFromCat.contains("vUpload") || sqlFile.isDirectory()) && StringUtils.isBlank(subjectNameFromCat)) {
                    String subjectName = sqlFile.getName();
                    try {
                        if (subjectName.contains(".")) {
                            subjectName = subjectName.substring(0, subjectName.lastIndexOf("."));
                            subjectName = subjectName.replace(".", "_");
                        }
                        subjectName = subjectName.replaceAll("[^a-zA-Z0-9 _-]", "_");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    subjectId = createSubject(subjectName, projectId, mappingManagerUtil);
                }

                mapName = sqlFile.getName();

                if (sqlFile.isDirectory()) {

                    callSubDirctory(sqlFile, subjectId, projectId, mappingManagerUtil, deleteOrArchiveSourceFile, filePathFromCat, subjectNameFromCat);

                } else if (!StringUtils.isBlank(subjectNameFromCat)) {

                    fileFoldersPathList.add(sqlFile.getAbsolutePath() + "##" + subjectId);

                } else {
                    if (!filePathFromCat.contains("vUpload")) {
                        subjectId = -1;
                    }

                    fileFoldersPathList.add(sqlFile.getAbsolutePath() + "##" + subjectId);

                }

            }

            String folderServerName = "";
            String folderDataBaseName = "";
//            boolean fileSelectionFlag = false;
            defaultSystemName = sysenvDetails[0];
            defaultEnviormentName = sysenvDetails[1];
            defaultSchemaName = defaultSchemaNameFromCat;
            String fileName = "";

            String schemaNameFromFolder = "";

            if (fileFoldersPathList.isEmpty()) {
                completeStatus = "No files are thier in path to create mappings";
            }

            List<String> mapNamesList = new ArrayList();
            for (String subFilePath : fileFoldersPathList) {
                exceptionBuilder = new StringBuilder();
                int i = 0;
                boolean flag = false;

                StringBuilder outerExceptionBuilder = new StringBuilder();
                String filePath = "";
                try {

                    if (subFilePath.contains("##")) {

                        int length = subFilePath.split("##").length;

                        if (length >= 1) {
                            filePath = subFilePath.split("##")[0];
                            String subjectStringId = subFilePath.split("##")[1];
                            subjectStringId = subjectStringId.replace("\"", "");
                            subjectId = Integer.parseInt(subjectStringId);

                            if (filePath.contains("\\")) {

                                filePath = filePath.replace("\\", "/");

                            }

                            mapName = filePath.substring(filePath.lastIndexOf("/") + 1);
                            fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
                            fileName = fileName.substring(0, fileName.lastIndexOf("."));
                            if (mapName.contains(".sql")) {
                                mapName = mapName.replace(".sql", "");
                            }
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    outerExceptionBuilder.append("Exception Details: " + e + "\n");
                }

                Set<String> storeProceduresSet = null;
                int exceptionCount = 0;
                try {

                    if (filePath.endsWith(".sql") || filePath.endsWith(".txt") || filePath.endsWith(".SQL") || filePath.endsWith(".TXT")) {
                        storeProceduresSet = ErwinSQLWrapper.getAllStatementsForSqlFile(filePath, databaseType);

                    } else {
                        String execeptionDeatils = "{\"Output\":{\"statusDescription\":\"Error\",\"statusNumber\":0,\"requestSuccess\":true,\"userObject\":null,\"id\":null,\"statusMessage\":\"Not able to parse the Query\"," + "\n";

                        execeptionDeatils = execeptionDeatils + "\"FileName \" :" + "\"" + fileName + "\"" + ",\n";
                        execeptionDeatils = execeptionDeatils + "\"FilePath \" :" + "\"" + filePath + "\"" + ",\n";
                        execeptionDeatils = execeptionDeatils + "\"MapName \" :" + "\"" + "null" + "\"" + "}\n}";

                        outerExceptionBuilder.append(execeptionDeatils);
                        execeptionDeatils = "";
                        flag = true;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    storeProceduresSet = null;
                    outerExceptionBuilder.append("Exception Details: " + e + "\n");
                }

                try {

                    if (!filePath.contains("vUpload")) {

                        if (filePath.contains("\\")) {
                            filePath = filePath.replaceAll("\\\\", "/");
                        } else {
                            filePath = filePath;
                        }
                        String filePathSpilt[] = filePath.split("/");
                        if (filePathSpilt.length > 4) {
                            schemaNameFromFolder = filePathSpilt[filePathSpilt.length - 2];
                            folderDataBaseName = filePathSpilt[filePathSpilt.length - 3];
                            folderServerName = filePathSpilt[filePathSpilt.length - 4];
                        }

                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    outerExceptionBuilder.append("Exception Details: " + e + "\n");
                }

                if (storeProceduresSet != null) {

                    Iterator<String> itr = storeProceduresSet.iterator();

                    useLine = "";
                    String sqltext = "";

                    String mapNameFromfile = "";

                    while (itr.hasNext()) {
                        exceptionBuilder = new StringBuilder();
                        exceptionBuilder.append(outerExceptionBuilder.toString());

                        mapName = filePath.substring(filePath.lastIndexOf("/") + 1);
                        if (mapNameFromfile.isEmpty() && mapName.contains(".sql")) {
                            mapName = mapName.replace(".sql", "");
                        } else {
                            mapName = mapNameFromfile;
                        }

                        sqltext = itr.next();
                        status = "";

                        try {
                            if (sqltext.trim().startsWith("USE")) {
                                useLine = sqltext.split(" ")[1].trim().replace("[", "").replace("]", "").toUpperCase();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            exceptionBuilder.append("Exception Details: " + e + "\n");
                        }

                        //sqltext = formatterofSql(sqltext, databaseType);
                        sqltext = removeUnparsedDataFromQuery(sqltext);

                        if (!StringUtils.isBlank(sqltext)) {
                            if (sqltext.toUpperCase().startsWith("CREATE") || sqltext.toUpperCase().startsWith("ALTER")) {
                                String[] arr = sqltext.split("\\s+");
                                try {
                                    mapName = arr[2].replace(".", "_").replace("]", "").replace("[", "");
                                    mapNameFromfile = mapName;

                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                    exceptionBuilder.append("Exception Details: " + ex + "\n");
                                }
                            }
                        }
                        mapName = mapName.replaceAll("[^a-zA-Z0-9 _-]", "_");

                        dataflow dtflow = getDataflowFromSql(sqltext, mapName, databaseType);
                        if (dtflow == null) {
                            String execeptionDeatils = "{\"Output\":{\"statusDescription\":\"Error\",\"statusNumber\":0,\"requestSuccess\":true,\"userObject\":null,\"id\":null,\"statusMessage\":\"Not able to parse the Query\"," + "\n";

                            execeptionDeatils = execeptionDeatils + "\"FileName\" :" + "\"" + fileName + "\"" + ",\n";
                            execeptionDeatils = execeptionDeatils + "\"FilePath\" :" + "\"" + filePath + "\"" + ",\n";
                            execeptionDeatils = execeptionDeatils + "\"MapName\" :" + "\"" + "null" + "\"" + "}\n}";
                            exceptionBuilder.append(execeptionDeatils);
                            execeptionDeatils = "";
                            exceptionCount++;
                        }
                        RelationAnalyzer relationAnalyzer = new RelationAnalyzer();

                        ArrayList<MappingSpecificationRow> mapSpecRows = null;
                        if (dtflow != null) {
//                            mapSpecRows = relationAnalyzer.analyzeRelations(dtflow, sysenvDetails, this.tableSystemEnvMap, this.metadataTableColumnDetailsMap);
                            mapSpecRows = relationAnalyzer.analyzeRelations(dtflow, sysenvDetails);
                            keyValuesDeailsMap = relationAnalyzer.getKeyValuesMap();
                        }

                        if (dtflow != null && (mapSpecRows == null || mapSpecRows.isEmpty())) {
                            String execeptionDeatils = "{\"Output\":{\"statusDescription\":\"Error\",\"statusNumber\":0,\"requestSuccess\":true,\"userObject\":null,\"id\":null,\"statusMessage\":\"Not able to parse the Query\"," + "\n";

                            execeptionDeatils = execeptionDeatils + "\"FileName \" :" + "\"" + fileName + "\"" + ",\n";
                            execeptionDeatils = execeptionDeatils + "\"FilePath \" :" + "\"" + filePath + "\"" + ",\n";
                            execeptionDeatils = execeptionDeatils + "\"MapName \" :" + "\"" + "null" + "\"" + "}\n}";

                            exceptionBuilder.append(execeptionDeatils);
                            execeptionDeatils = "";
                            exceptionCount++;
                        }
                        if (mapNamesList.contains(mapName + subjectId)) {
                            mapName = mapName + "_" + ++i;
                        }
                        mapNamesList.add(mapName + subjectId);

                        if (mapSpecRows != null && mapSpecRows.size() > 0) {
                            mapping = new Mapping();
                            mapping.setMappingName(mapName);
                            mapping.setProjectId(projectId);
                            mapping.setSubjectId(subjectId);
                            mapping.setMappingSpecifications(mapSpecRows);
                            mapping.setSourceExtractQuery(sqltext);

                            mappingJson = objectMapper.writeValueAsString(mapping);

                            if (mappingJson != null || !mappingJson.isEmpty()) {
                                status = createMappingFromJson(mappingJson, mapName, subjectId, projectId, mappingManagerUtil, jsonFilePath, folderServerName, folderDataBaseName, schemaNameFromFolder, fileName, loadType, executionDateTime, logFilePath, filePath, sqltext, deleteOrArchiveSourceFile, filePathFromCat, archivePath) + "\n";

                            }

                        }

                        completeStatus = completeStatus + " " + status + "\n";
                        try {
                            if (sqltext.contains("\nUSE")) {

                                String useLineSpilt[] = sqltext.split("\nUSE");
                                if (useLineSpilt.length >= 2) {
                                    useLine = useLineSpilt[1].split(" ")[1].trim().replace("[", "").replace("]", "").toUpperCase();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            exceptionBuilder.append("Exception Details: " + e + "\n");
                        }
                        String logFileName = "SQL Parser log_";
                        String fileExtension = "log";
                        createFile(logFilePath, executionDateTime, status, "logData", logFileName, fileExtension);

                    }
                }

                try {
                    int storeProcSize = 0;
                    if (storeProceduresSet != null) {
                        storeProcSize = storeProceduresSet.size();
                    }

                    String individualStatus = "";
                    individualStatus = writeExeceptionDeatilsIntoFile(flag, storeProcSize, exceptionCount, outerExceptionBuilder, exceptionBuilder, logFilePath, executionDateTime);

                    completeStatus = completeStatus + " " + individualStatus + "\n";
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

        } catch (Exception ex) {
            ex.printStackTrace();
            exceptionBuilder.append("Exception Details: " + ex + "\n");
        }

        return completeStatus;

    }

    public static void callSubDirctory(File directories, int parentSubjectId, int projectId, MappingManagerUtil mappingManagerUtil, String deleteOrArchiveSourceFile, String filePathFromCat, String subjectNameFromCat) {

        File[] subDirectories = directories.listFiles();
        int length = subDirectories.length;
        int count = 0;

        for (File subFile : subDirectories) {
            String subjectName = subFile.getName();
            count++;

            try {

                if (subFile.isDirectory()) {

                    try {

                        if (StringUtils.isBlank(subjectNameFromCat)) {
                            if (subjectName.contains(".")) {
                                subjectName = subjectName.substring(0, subjectName.lastIndexOf("."));
                                subjectName = subjectName.replace(".", "_");

                            }

                            subjectName = subjectName.replaceAll("[^a-zA-Z0-9 _-]", "_");
                            childSubjectId = createChildSubject(subjectName, projectId, parentSubjectId, mappingManagerUtil);
                        }

                    } catch (Exception e) {

                        e.printStackTrace();

                    }
                    if (count >= length) {
                        parentSubjectId = childSubjectId;
                    }

                    callSubDirctory(subFile, childSubjectId, projectId, mappingManagerUtil, deleteOrArchiveSourceFile, filePathFromCat, subjectNameFromCat);
                } else {
                    String filePathAndSubjectId = "";
                    if (StringUtils.isBlank(subjectNameFromCat)) {
                        filePathAndSubjectId = subFile.getAbsolutePath() + "##" + parentSubjectId;
                    } else {
                        filePathAndSubjectId = subFile.getAbsolutePath() + "##" + subjectCatOptionId;
                    }

                    fileFoldersPathList.add(filePathAndSubjectId);

                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static void archiveSourceFiles(String filePath, String deleteOrArchiveSourceFile, String filePathFromCat, String archivePathFromCat) {

        if (filePath.contains("\\")) {
            filePath = filePath.replaceAll("\\\\", "/");
        }
        if (filePathFromCat.contains("\\")) {
            filePathFromCat = filePathFromCat.replaceAll("\\\\", "/");
        }

        if (filePathFromCat.endsWith("/")) {
            filePathFromCat = filePathFromCat.substring(0, filePathFromCat.lastIndexOf("/"));
        }

        File actualFilePath = new File(filePath);
        if (deleteOrArchiveSourceFile.equalsIgnoreCase("archiveSourceFile")) {

            String filePathSpilt[] = filePath.split("/");
            String erwinName = "";
            String archivePath = "";
            String erwinSpiltPath[] = null;

            try {
                if (filePathSpilt.length > 6 && filePath.toLowerCase().contains("erwin/ms sql source")) {
                    erwinName = filePathSpilt[filePathSpilt.length - 6];

                    erwinSpiltPath = filePath.split(erwinName);
                    if (erwinSpiltPath.length >= 2) {
//                        archivePath = filePath.split(erwinName)[0] + erwinName + "/archive" + "/" + filePath.split(erwinName)[1];
                        archivePath = archivePathFromCat + filePath.split(erwinName)[1];
                        archivePath = archivePath.substring(0, archivePath.lastIndexOf("/") + 1);
                    }

                } else {

                    filePathFromCat = filePathFromCat.substring(filePathFromCat.lastIndexOf("/") + 1);
                    erwinSpiltPath = filePath.split(filePathFromCat);
                    if (erwinSpiltPath.length >= 2) {

//                        archivePath = erwinSpiltPath[0] + "archive/MS Sql Source/" + filePathFromCat + erwinSpiltPath[1];
                        archivePath = archivePathFromCat + "MS Sql Source/" + filePathFromCat + erwinSpiltPath[1];
                    }

                    archivePath = archivePath.substring(0, archivePath.lastIndexOf("/") + 1);

                }
            } catch (Exception e) {
                e.printStackTrace();
                exceptionBuilder.append("Exception Details: " + e + "\n");
            }
//            archivePath = "C:test/abc.txt";
//            archivePath = "C:erwin/test/TEST";
//            filePath = "C:erwin/test/temp.sql";
//            String output = "C:erwin/test/TEST/temp.sql";
//            output= "C:erwin/test/TEST/TEST/temp.sql";

            File archiveFilePath = null;
            if (!StringUtils.isBlank(archivePath)) {
                archiveFilePath = new File(archivePath);
                if (!archiveFilePath.exists()) {
                    archiveFilePath.mkdirs();
                }
            }

            try {
                if (archiveFilePath != null) {
                    FileUtils.copyFileToDirectory(actualFilePath, archiveFilePath);
                }

            } catch (Exception e) {
                e.printStackTrace();
                exceptionBuilder.append("Exception Details: " + e + "\n");
            }

        }

    }

    public dataflow getDataflowFromSql(String sqltext, String fileName, String databaseType) {

        try {

//            TGSqlParser sqlparser = isQueryParsable(sqltext);
            EDbVendor dbVendor = getDBVendorFromStringVendorName(databaseType);
            TGSqlParser sqlparser = isQueryParsable(sqltext, dbVendor);
            if (sqlparser == null) {
                System.out.println("Query is Not able to parse" + "...." + "FileName...." + fileName);
                return null;
            } else {
                System.out.println("Query is Compatible to " + sqlparser.getDbVendor() + "...." + "FileName...." + fileName);
            }
//            String xml = DataFlowAnalyzer_Debugger.getanalyzeXmlforsql(sqltext, fileName, sqlparser);
//
////            File xmlFile = new File("C:\\Users\\Output_V5.xml");
////            FileUtils.writeStringToFile(xmlFile, xml, "UTF-8");
//            return XML2Model.loadXML(dataflow.class, xml);
            DataFlowAnalyzer dlineage = new DataFlowAnalyzer(sqltext, fileName, EDbVendor.dbvmssql, false);
            dlineage.setShowJoin(true);
            dlineage.setIgnoreRecordSet(false);

            StringBuffer errorBuffer = new StringBuffer();
            String result = dlineage.generateDataFlow(errorBuffer);

//            File xmlFile = new File("C:\\Users\\TrineshVanguri\\Desktop\\Version8_3.xml");
//            FileUtils.writeStringToFile(xmlFile, result, "UTF-8");
            return XML2Model.loadXML(dataflow.class, result);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static String createMappingFromJson(String mappingJson, String mapName, int subjectId, int projectId, MappingManagerUtil maputil, String jsonFilePath, String folderServerName, String folderDatabaseName, String schemaNameFromFolder, String fileName, String loadType, String executionDateTime, String logFilePath, String filePath, String sqlText, String deleteOrArchiveSourceFile, String filePathFromCat, String archivePath) {
        String status = "";
        Mapping mapping = null;
        try {
            try {
                folderServerName = folderServerName.replaceAll("[^a-zA-Z0-9]", "_");
                folderDatabaseName = folderDatabaseName.replaceAll("[^a-zA-Z0-9]", "_");
                useLine = useLine.replaceAll("[^a-zA-Z0-9]", "_");
            } catch (Exception e) {
                e.printStackTrace();
                exceptionBuilder.append("Exception Details: " + e + "\n");
            }

            String schemaName = "";
            if (StringUtils.isBlank(schemaNameFromFolder)) {
                schemaName = defaultSchemaName;
            } else {
                schemaName = schemaNameFromFolder;
            }

            folderDatabaseName = folderDatabaseName + "separatorUseLine" + useLine;

            ArrayList<MappingSpecificationRow> SpecsLists = SyncMetadataJsonFileDesign.setMetaDataSpec(mappingJson, folderDatabaseName, folderServerName, jsonFilePath, defaultSystemName, defaultEnviormentName, metadatChacheHM, allTablesMap, allDBMap, schemaName, logFilePath);

            mapping = new Mapping();
            mapping.setMappingName(mapName);
            mapping.setProjectId(projectId);
            mapping.setSubjectId(subjectId);
            mapping.setMappingSpecifications(SpecsLists);
            mapping.setSourceExtractQuery(sqlText);

            ObjectMapper objectMapper = new ObjectMapper();
            mappingJson = objectMapper.writeValueAsString(mapping);

            mappingJson = mappingJson.replace(",\"childNodes\":[]", "");

            int mappingId = -1;

            try {

                mappingId = getMappingId(subjectId, mapName, projectId, maputil);

            } catch (Exception e) {
                e.printStackTrace();
                exceptionBuilder.append("Exception Details: " + e + "\n");
            }

            if (mappingId > 0 && loadType.equalsIgnoreCase("full")) {
                String deleteStatus = "";
                if (subjectId > 0) {
                    deleteStatus = maputil.deleteMappingAs(subjectId, "MM_SUBJECT", mapName, "ALL_VERSIONS", 0.0f, "json");
                } else {
                    deleteStatus = maputil.deleteMappingAs(projectId, "MM_PROJECT", mapName, "ALL_VERSIONS", 0.0f, "json");
                }

                deleteStatus = getStatusWithFileAndMapName(deleteStatus, fileName, mapName, loadType, filePath);
                String createStatus = maputil.createMappingAs(mappingJson, "json");
                createStatus = getStatusWithFileAndMapName(createStatus, fileName, mapName, loadType, filePath);

                mappingId = getMappingId(subjectId, mapName, projectId, maputil);

                status = status + "\n" + deleteStatus + "\n" + createStatus;

            } else if (mappingId > 0 && loadType.equalsIgnoreCase("incremental")) {
//                String versionStatusWithLatestMapId = CreateMappingVersion.preCreatingMapVersion(mappingJson, projectId, mapName, maputil, subjectId, kvUtil);

                String versionStatusWithLatestMapId = CreateMappingVersion.creatingMapVersionForIncremental(projectId, mapName, subjectId, SpecsLists, maputil, kvUtil);
                String versionStatus = "";

                if (versionStatusWithLatestMapId.contains("##") && versionStatusWithLatestMapId.split("##").length >= 2) {
                    versionStatus = versionStatusWithLatestMapId.split("##")[0];
                    String mapStringId = versionStatusWithLatestMapId.split("##")[1];
                    mapStringId = mapStringId.replace("\"", "");
                    mappingId = Integer.parseInt(mapStringId);

                }
                status = status + "\n" + getStatusWithFileAndMapName(versionStatus, fileName, mapName, loadType, filePath) + "\n";
            } else {
                String newStatus = maputil.createMappingAs(mappingJson, "JSON");
                status = status + "\n" + getStatusWithFileAndMapName(newStatus, fileName, mapName, loadType, filePath) + "\n";

                try {

                    mappingId = getMappingId(subjectId, mapName, projectId, maputil);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (mappingId > 0) {

                if (deleteOrArchiveSourceFile.equalsIgnoreCase("archiveSourceFile") && !(filePathFromCat.contains("vUpload"))) {

                    archiveSourceFiles(filePath, deleteOrArchiveSourceFile, filePathFromCat, archivePath);
                }
                if (deleteOrArchiveSourceFile.equalsIgnoreCase("archiveSourceFile") || deleteOrArchiveSourceFile.equalsIgnoreCase("deleteSourceFile")) {
                    File actualFilePathFile = new File(filePath);
                    actualFilePathFile.delete();
                }

            }

            try {
//                status = status + addKeyValues(mappingId, keyValuesDeailsMap, kvUtil);
                addKeyValues(mappingId, keyValuesDeailsMap, kvUtil);

            } catch (Exception e) {
                e.printStackTrace();
                exceptionBuilder.append("Exception Details: " + e + "\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
            exceptionBuilder.append("Exception Details: " + e + "\n");
        }
        return status;
    }

    // Added By Dinesh On June_22_2020
    public static int createSubject(String subjectName, int projectId, MappingManagerUtil maputil) {
        try {
            Project project = maputil.getProject(projectId);
            String projectName = project.getProjectName();
            int subjectId = maputil.getSubjectId(projectName, subjectName);

            if (subjectId > 0) {
                return subjectId;
            }

            Subject subjectDetails = new Subject();
            subjectDetails.setSubjectName(subjectName);
            subjectDetails.setSubjectDescription("Oracle and sql Details");
            subjectDetails.setProjectId(projectId);
            subjectDetails.setConsiderUserDefinedFlag("Y");
            subjectDetails.setParentSubjectId(-1);

            RequestStatus retRS = maputil.createSubject(subjectDetails);
            if (retRS.isRequestSuccess()) {

                subjectId = maputil.getSubjectId(projectName, subjectName);
                return subjectId;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    private TGSqlParser isQueryParsable(String sqltext, EDbVendor vendor) {
        TGSqlParser sqlparser = new TGSqlParser(vendor);
        sqlparser.sqltext = sqltext;
        int parsedResult = sqlparser.parse();

        if (parsedResult == 0) {
            return sqlparser;
        } else {
            return null;
        }
    }

    public static String removeUnparsedDataFromQuery(String sqlFileContent) {
        String parsedData = "";
        boolean flag = true;
        try {

            String spiltSqlFileData[] = sqlFileContent.split("\n");

            for (String data : spiltSqlFileData) {

                if (StringUtils.isBlank(data)) {
                    continue;
                }
                // the 1st space in the downline is some special space character from the query that's why the query is not parsing so we replaced
                // the special space character with normal space character in the downline
                data = data.replaceAll(" ", " ");
                data = data.toUpperCase().replace("FROM ISNULL", "+ ISNULL");

                try {
                    if (data.startsWith("USE")) {
                        useLine = data.split(" ")[1].trim().replace("[", "").replace("]", "").toUpperCase();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    exceptionBuilder.append("Exception Details: " + e + "\n");
                }

                if (data.toUpperCase().contains("OPTION") && data.toUpperCase().contains("USE")) {

                    try {
//                        data = data.substring(0, data.toUpperCase().indexOf("OPTION"));

                        data = data.toUpperCase().replace("OPTION", ";--OPTION");
//                        if (flag) {
                        parsedData = parsedData + data + "\n";
//                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

//                    continue;
                } else if (!data.trim().toUpperCase().startsWith("CREATE") && data.toUpperCase().contains("INDEX") && data.toUpperCase().contains("CLUSTERED")) {
                    continue;
                } else if (data.trim().toUpperCase().startsWith("ALTER TABLE")) {
                    flag = false;

                    continue;
                } else if (data.trim().toUpperCase().contains("PERCENTILE_DISC(.5)")) {
                    continue;
                } else {

                    try {
                        if (data.contains("CASE") && data.contains("?") || data.contains("Â€‹") || data.contains("â€‹")) {//Â€‹
                            data = data.replaceAll("\\?", "").replaceAll("Â€‹", "").replaceAll(" CASE", "CASE").replaceAll("â€‹", "");
                        }

                        data = data.toUpperCase().replaceAll("AT TIME ZONE 'EASTERN STANDARD TIME'", "");
                        data = data.toUpperCase().replaceAll("FOR SYSTEM_TIME ALL", "");
                        data = data.toUpperCase().replaceAll("RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", "");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

//                    }
                    if (data.toUpperCase().contains("SELECT") || data.toUpperCase().contains("UPDATE")
                            || data.toUpperCase().contains("DELETE") || data.toUpperCase().contains("INSERT")
                            || data.toUpperCase().contains("SET") || data.toUpperCase().contains("DECLARE")
                            || data.toUpperCase().contains("PRINT") || data.toUpperCase().contains("TRUNCATE") || data.toUpperCase().contains("IF EXISTS")) {
                        flag = true;
                    }
                    if (flag) {
                        parsedData = parsedData + data + "\n";
                    }

                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            exceptionBuilder.append("Exception Details: " + e + "\n");

        }
        return parsedData;
    }

    public static String addKeyValues(int mappingId, LinkedHashMap<String, HashSet<String>> keyValuesHashMap, KeyValueUtil keyValueUtil) {
        String status = "";
        try {

            if (mappingId > 0 && keyValuesHashMap != null && !keyValuesHashMap.isEmpty()) {
                Set<String> conditionTypeSet = keyValuesHashMap.keySet();
                Iterator<String> ctItr = conditionTypeSet.iterator();

                List<KeyValue> keyValuesList = new ArrayList();
                while (ctItr.hasNext()) {
                    String conditionType = ctItr.next();
                    List<KeyValue> tkv = addSpecificKeyValues(keyValuesHashMap.get(conditionType), getKeyType(conditionType));

                    if (tkv != null && !tkv.isEmpty()) {
                        keyValuesList.addAll(tkv);
                    }

                }
                if (keyValuesList != null && !keyValuesList.isEmpty()) {

                    RequestStatus rs = keyValueUtil.addKeyValues(keyValuesList, Node.NodeType.MM_MAPPING, mappingId);
                    status = status + rs.getStatusMessage();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static String getKeyType(String conditionType) {
        if ("JOIN_CONDITION".equals(conditionType)) {
            return "Join";
        } else if ("WHERE_CONDITION".equals(conditionType)) {
            return "Where condition";
        } else if ("GROUPBY_CONDITION".equals(conditionType)) {
            return "Group by";
        } else if ("ORDERBY_CONDITION".equals(conditionType)) {
            return "Order by";
        } else {
            return "CONDITION";
        }

    }

    public static List<KeyValue> addSpecificKeyValues(Set<String> conditionsSet, String pKeyType) {

        if (conditionsSet != null && !conditionsSet.isEmpty()) {

            List<KeyValue> keyValuesList = new ArrayList();
            Iterator<String> cItr = conditionsSet.iterator();
            int icr = 0;
            while (cItr.hasNext()) {

                String condition = cItr.next();
                String keyType = pKeyType;
                //adding join type
                if ("Join".equalsIgnoreCase(keyType)) {
                    String[] tempCndArr = condition.split(DELIMITER);
                    condition = tempCndArr[0];
                    if (!"Join".equalsIgnoreCase((tempCndArr[1]).trim())) {
                        keyType = tempCndArr[1] + " " + keyType;
                    }
                }

                if (condition != null && condition.length() > 0) {

                    condition = removeSpaces(condition);
                    String key = keyType + " " + (++icr);

                    //if value length is more than 500, we are to split and adding
                    if (condition.length() > 500) {
                        int j = 0;

                        if (StringUtils.isBlank(key)) {
                            continue;
                        }
                        String value = "";
                        while (condition.length() > 500) {
                            value = condition.substring(0, 500);
                            if (value == null) {
                                continue;
                            }

                            keyValuesList.add(buildKeyValue(key + " part_" + (++j), value));
                            condition = condition.substring(500);
                        }
                        value = condition;
                        if (value == null) {
                            continue;
                        }
                        keyValuesList.add(buildKeyValue(key + " part_" + (++j), value));
                    } else {
                        //    String value = condition;
                        if (condition == null) {
                            continue;
                        }
                        keyValuesList.add(buildKeyValue(key, condition));
                    }
                }

            }
            return keyValuesList;
        }
        return null;
    }

    public static String removeSpaces(String str) {
        return str.trim().replace("\t", " ").replaceAll("( )+", " ");
    }

    public static KeyValue buildKeyValue(String key, String value) {

        KeyValue kv = new KeyValue(key, value);
        kv.setPublished(true);
        kv.setVisibility(1);
        return kv;
    }

    public static int createChildSubject(String subjectName, int projectId, int parentSubId, MappingManagerUtil mappingManagerUtil) {
        StringBuilder sb = new StringBuilder();
        Subject subjectDetails = new Subject();
        int subjectId = 0;

        try {

            subjectId = mappingManagerUtil.getSubjectId(parentSubId, Node.NodeType.MM_SUBJECT, subjectName);
            if (subjectId > 0) {
                return subjectId;
            }
        } catch (Exception e) {

        }

        //  Subject subjectDetails = new Subject();
        subjectDetails.setSubjectName(subjectName);
        subjectDetails.setSubjectDescription("Oracle and sql Details_Child");
        subjectDetails.setProjectId(projectId);
        subjectDetails.setConsiderUserDefinedFlag("Y");
        subjectDetails.setParentSubjectId(parentSubId);

        try {
            RequestStatus retRS = mappingManagerUtil.createSubject(subjectDetails);
            sb.append(subjectName + " " + retRS.getStatusMessage() + "\n\n");
//            if (retRS.isRequestSuccess()) {

            subjectId = mappingManagerUtil.getSubjectId(parentSubId, Node.NodeType.MM_SUBJECT, subjectName);
            //     subjectId = mappingManagerUtil.getSubjectId(projectName, subjectName);
            return subjectId;
//            }
        } catch (Exception e) {

        }
        return subjectId;
    }

    public static String getStatusWithFileAndMapName(String status, String fileName, String mapName, String loadType, String filePath) {
        try {

            if (status.contains("}")) {

                String statusSpilt[] = status.split("}");

                status = statusSpilt[0].trim() + "," + "\n";
                status = status + " \"FileName \" : " + "\"" + fileName + "\"" + "," + "\n";
                status = status + " \"FilePath \" : " + "\"" + filePath + "\"" + "," + "\n";

                status = status + " \"MapName \": " + "\"" + mapName + "\"" + "\n" + "}" + "\n" + "}";

            } else if (loadType.equalsIgnoreCase("incremental")) {

                status = " \"statusMessage \" :" + status + "," + "\n";
                status = status + " \"FileName \" : " + "\"" + fileName + "\"" + "," + "\n";
                status = status + " \"FilePath \" : " + "\"" + filePath + "\"" + "," + "\n";

                status = "{\n" + status + " \"MapName \": " + "\"" + mapName + "\"" + " \n}";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static int getMappingId(int subjectId, String mapName, int projectId, MappingManagerUtil mappingManagerUtil) {
        int mappingId = 0;
        try {
            if (subjectId > 0) {
                mappingId = mappingManagerUtil.getMappingId(subjectId, mapName, Node.NodeType.MM_SUBJECT);
            } else {
                mappingId = mappingManagerUtil.getMappingId(projectId, mapName, Node.NodeType.MM_PROJECT);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return mappingId;
    }

    public static void createFile(String filePath, String executionDateTime, String content, String fileType, String fileName, String fileExtension) {
        BufferedWriter bufferedWriter = null;
        FileWriter fileWriter = null;
        File file = null;

        try {
            String outputMetadataFilePath = "";
            if (fileType.equalsIgnoreCase("logData")) {
                file = new File(filePath);
                if (!file.exists()) {
                    file.mkdirs();
                }
                outputMetadataFilePath = filePath + "/" + fileName + executionDateTime + "." + fileExtension;
            } else if (fileType.equalsIgnoreCase("archiveFile")) {
                outputMetadataFilePath = filePath + fileName + "." + fileExtension;
            }

            file = new File(outputMetadataFilePath);

            fileWriter = new FileWriter(file, true);

            bufferedWriter = new BufferedWriter(fileWriter);

            bufferedWriter.write(content);
            bufferedWriter.flush();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                if (fileWriter != null) {
                    fileWriter.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (bufferedWriter != null) {
                    bufferedWriter.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    public static EDbVendor getDBVendorFromStringVendorName(String dbName) {
        EDbVendor dbVendor = EDbVendor.dbvmssql;
        if ("oracle".equals(dbName)) {
            dbVendor = EDbVendor.dbvoracle;
        } else if ("mssql".equals(dbName)) {
            dbVendor = EDbVendor.dbvmssql;
        } else if ("postgresql".equals(dbName)) {
            dbVendor = EDbVendor.dbvpostgresql;
        } else if ("redshift".equals(dbName)) {
            dbVendor = EDbVendor.dbvredshift;
        } else if ("odbc".equals(dbName)) {
            dbVendor = EDbVendor.dbvodbc;
        } else if ("mysql".equals(dbName)) {
            dbVendor = EDbVendor.dbvmysql;
        } else if ("netezza".equals(dbName)) {
            dbVendor = EDbVendor.dbvnetezza;
        } else if ("firebird".equals(dbName)) {
            dbVendor = EDbVendor.dbvfirebird;
        } else if ("access".equals(dbName)) {
            dbVendor = EDbVendor.dbvaccess;
        } else if ("ansi".equals(dbName)) {
            dbVendor = EDbVendor.dbvansi;
        } else if ("generic".equals(dbName)) {
            dbVendor = EDbVendor.dbvgeneric;
        } else if ("greenplum".equals(dbName)) {
            dbVendor = EDbVendor.dbvgreenplum;
        } else if ("hive".equals(dbName)) {
            dbVendor = EDbVendor.dbvhive;
        } else if ("sybase".equals(dbName)) {
            dbVendor = EDbVendor.dbvsybase;
        } else if ("hana".equals(dbName)) {
            dbVendor = EDbVendor.dbvhana;
        } else if ("impala".equals(dbName)) {
            dbVendor = EDbVendor.dbvimpala;
        } else if ("dax".equals(dbName)) {
            dbVendor = EDbVendor.dbvdax;
        } else if ("vertica".equals(dbName)) {
            dbVendor = EDbVendor.dbvvertica;
        } else if ("couchbase".equals(dbName)) {
            dbVendor = EDbVendor.dbvcouchbase;
        } else if ("snowflake".equals(dbName)) {
            dbVendor = EDbVendor.dbvsnowflake;
        } else if ("openedge".equals(dbName)) {
            dbVendor = EDbVendor.dbvopenedge;
        } else if ("informix".equals(dbName)) {
            dbVendor = EDbVendor.dbvinformix;
        } else if ("teradata".equals(dbName)) {
            dbVendor = EDbVendor.dbvteradata;
        } else if ("mdx".equals(dbName)) {
            dbVendor = EDbVendor.dbvmdx;
        } else if ("db2".equals(dbName)) {
            dbVendor = EDbVendor.dbvdb2;
        }
        return dbVendor;
    }

    public static void search(String pattern, File folder, String[] sysEnvDetails) {
        for (File f : folder.listFiles()) {
            if (f.isDirectory()) {
                search(pattern, f, sysEnvDetails);
            }
            if (f.isFile()) {
                if (f.getName().matches(pattern)) {
                    System.out.println("------------- FileName is--->" + f.getAbsolutePath() + "<----------------");
//                    MappingCreator_BeforeDeletingSourceFiles mappingCreator = new MappingCreator_BeforeDeletingSourceFiles();
//                    mappingCreator.getMappingSpecList(f.getAbsolutePath(), sysEnvDetails);
                }
            }

        }
    }

    public ArrayList<MappingSpecificationRow> getMappingSpecList(String inputFilePath, String[] sysenvDetails) {
        try {

            File inputFile = new File(inputFilePath);
            //String fileName = getFileName(inputFilePath);
            String fileName = "Twes";
            String sqltext = FileUtils.readFileToString(inputFile, "UTF-8");

            dataflow dtflow = getDataflowFromSql(sqltext, fileName, "mssql");
            if (dtflow == null) {
                return new ArrayList<>();
            }
            com.erwin.sqlparser.RelationAnalyzer relationAnalyzer = new com.erwin.sqlparser.RelationAnalyzer();

//            ArrayList<MappingSpecificationRow> mapSpecRows = relationAnalyzer.analyzeRelations(dtflow, sysenvDetails, this.tableSystemEnvMap, this.metadataTableColumnDetailsMap);
            ArrayList<MappingSpecificationRow> mapSpecRows = relationAnalyzer.analyzeRelations(dtflow, sysenvDetails);
            this.keyValuesDeailsMap = relationAnalyzer.getKeyValuesMap();

            for (MappingSpecificationRow mspecRow : mapSpecRows) {
                System.out.println(mspecRow.getSourceTableName() + "====>" + mspecRow.getSourceColumnName() + "====>"
                        + mspecRow.getBusinessRule() + "===>" + mspecRow.getTargetTableName() + "====>" + mspecRow.getTargetColumnName());
            }
            System.out.println("keyValuesMap ===> " + this.keyValuesDeailsMap);

            return mapSpecRows;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {

        String fileDirectoryPath = "D:\\Dinesh Code Base\\25SSIS REV ENGG_Puma\\25TestFolderForSqlClass";

        String[] sysEnvDetails = {"SrcSystem", "SrcEnv", "TgtSystem", "TgtEnv"};
            }

    public static String writeExeceptionDeatilsIntoFile(boolean flag, int storeProcSize, int exceptionCount, StringBuilder outerExceptionBuilder, StringBuilder exceptionBuilder, String logFilePath, String executionDateTime) {
        String individualStatus = "";
        try {

            if (flag || exceptionCount == storeProcSize) {

//                exceptionBuilder.length();
                if (exceptionCount == storeProcSize && exceptionBuilder.length() > 0) {
                    individualStatus = exceptionBuilder.toString();
                } else {
                    individualStatus = outerExceptionBuilder.toString();
                }

                individualStatus = individualStatus + "\n";
                String logFileName = "SQL Parser log_";
                String fileExtension = "log";
                createFile(logFilePath, executionDateTime, individualStatus, "logData", logFileName, fileExtension);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return individualStatus;

    }

    public static String getTheFilePathWithForwardSlash(String filePath) {

        try {
            if (!StringUtils.isBlank(filePath)) {
                if (filePath.contains("\\")) {
                    if (!filePath.endsWith("\\")) {
                        filePath = filePath.replace("\\", "/") + "/";
                    } else {
                        filePath = filePath.replace("\\", "/");
                    }
                } else {
                    if (!filePath.endsWith("/")) {
                        filePath = filePath + "/";
                    } else {
                        filePath = filePath;
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return filePath;
    }

    public static String formatterofSql(String sqlStatement, String databaseType) {
        try {
            EDbVendor dbVendor = getDBVendorFromStringVendorName(databaseType);
            TGSqlParser gSqlParser = new TGSqlParser(dbVendor);
            gSqlParser.setSqltext(sqlStatement);
            int ret = gSqlParser.parse();
            GFmtOpt option = GFmtOptFactory.newInstance();
            sqlStatement = FormatterFactory.pp(gSqlParser, option);

//sqlStatement = removeUnwantedCharatersFromFromTable(sqlStatement);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sqlStatement;
    }
}
