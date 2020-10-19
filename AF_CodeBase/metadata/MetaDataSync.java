/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.metadata;

import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.sm.SMColumn;
import com.ads.api.beans.sm.SMEnvironment;
import com.ads.api.util.SystemManagerUtil;
import com.erwin.SSISGeneric2015.ExtreamSourceAndExtreamTarget;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author AmanSingh
 */
public class MetaDataSync {

    private static final Logger LOGGER = Logger.getLogger(MetaDataSync.class);

    public static ArrayList<MappingSpecificationRow> syncMetaData(ArrayList<MappingSpecificationRow> mapSpecsList, List<String> envMap, SystemManagerUtil smutill, boolean isAppendHashForTable, boolean isNeedtoAddSchemaNameForOrphanTable, boolean isNeedtoChangeInsertSelect, boolean isNeedtoDoLookupWithStarcolumns, String mapName) {
        Map<String, String> sysEnvTableDetails = new HashMap<>();
        Set<String> tableNotFound = new HashSet<String>();
        Map<String, Map<String, Integer>> tableColDetails = new HashMap<>();
        Map<String, Map<Integer, SMColumn>> ColsDetails = new HashMap<>();
        String storeProcMapName="";
        String dataflowName = getdataFlowName(mapName, mapSpecsList);
        try {
            if (isAppendHashForTable) {
                appendHashforTable(mapSpecsList);
            }
            if (isNeedtoAddSchemaNameForOrphanTable) {
                addSchemaNameforOrphantables(mapSpecsList);
            }
            boolean createmapNameascomponent=false;
            Set<String> extremeTgttabset = ExtreamSourceAndExtreamTarget.getExtremeTargetSet(mapSpecsList);
                for (String extremeTgttable : extremeTgttabset) {
                   String componentName=extremeTgttable;
                   storeProcMapName=gettingStoreprocMapName(componentName, mapSpecsList);
                   storeProcMapName=storeProcMapName.trim();
                }
            changeresultofTarget(mapSpecsList);
            removebraces(mapSpecsList);
            if (isNeedtoChangeInsertSelect) {
                changeinsertselect(mapSpecsList);
            }
            if (isNeedtoDoLookupWithStarcolumns) {
                dolookupwithstarcolumns(mapSpecsList);
            }
             List<MappingSpecificationRow> colstars = updatespecrowforstar(mapSpecsList, envMap, sysEnvTableDetails, tableColDetails, smutill);
            mapSpecsList.addAll(colstars);
            List<MappingSpecificationRow> insertcols = settingstarColsofInsert(mapSpecsList);
            mapSpecsList.addAll(insertcols);
            removestarcolumspec(mapSpecsList);
            // String mapName = map.getMappingName();
            String storproc = mapName;
            String storprocName = storproc.replaceAll("[0-9]", "");
            if (mapName.contains(".")) {
                storproc = mapName.substring(0, mapName.lastIndexOf("."));
                storprocName = storproc.replaceAll("[0-9]", "");
            }
            for (MappingSpecificationRow mapSPecs : mapSpecsList) {
                String Sourcetablename = mapSPecs.getSourceTableName().toUpperCase().trim();
                if (Sourcetablename.split("\n").length == 1) {
                    if (Sourcetablename.split("\\.").length >= 3) {
                        Sourcetablename = Sourcetablename.split("\\.")[Sourcetablename.split("\\.").length - 2] + "." + Sourcetablename.split("\\.")[Sourcetablename.split("\\.").length - 1];
                    }
                }
                sourceNamesSet(Sourcetablename.toUpperCase().trim(), envMap, mapSPecs, sysEnvTableDetails, tableColDetails, smutill, mapName, dataflowName, tableNotFound,storeProcMapName);
                String targetTableName = mapSPecs.getTargetTableName().toUpperCase().trim();
                if (targetTableName.split("\n").length == 1) {
                    if (targetTableName.split("\\.").length >= 3) {
                        targetTableName = targetTableName.split("\\.")[targetTableName.split("\\.").length - 2] + "." + targetTableName.split("\\.")[targetTableName.split("\\.").length - 1];
                    }
                }
                targetNameSet(targetTableName.toUpperCase().trim(), envMap, mapSPecs, mapName, sysEnvTableDetails, tableColDetails, smutill, dataflowName, tableNotFound,storeProcMapName);
                if (!tableNotFound.contains(Sourcetablename.trim().toUpperCase())) {
                    addSourceColumnsDetails(mapSPecs, Sourcetablename.trim().toUpperCase(), sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
                }
                if (!tableNotFound.contains(targetTableName.trim().toUpperCase())) {
                    addTargetColumnsDetails(mapSPecs, targetTableName.trim().toUpperCase(), sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
                }
            };
        } catch (Exception e) {
            LOGGER.error("Exception Occured at MetaDataSync inside syncMetaData()>>", e);
        }
        

        return mapSpecsList;
    }

    private static void addTargetColumnsDetails(MappingSpecificationRow mapSpec, String columnName, String tableName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, Map<String, Map<Integer, SMColumn>> ColsDetails, List<String> sysEnvDetails, SystemManagerUtil smUtil) {

        try {
            SMColumn column = getColDetails(columnName, tableName, sysEnvTableDetails, tableColDetails, ColsDetails, sysEnvDetails, smUtil);
            if (column != null) {
                mapSpec.setTargetColumnDatatype(column.getColumnDatatype());
                mapSpec.setTargetColumnLength(StringUtils.isNumeric(column.getColumnLength()) ? Integer.parseInt(column.getColumnLength()) : 0);
                mapSpec.setTargetColumnPrecision(StringUtils.isNumeric(column.getColumnPrecision()) ? Integer.parseInt(column.getColumnPrecision()) : 0);
                mapSpec.setTargetColumnDefinition(column.getColumnDefinition());
                mapSpec.setTargetColumnNullableFlag(column.isColumnNullableFlag());
                mapSpec.setTargetColumnClass(column.getColumnClass());
                mapSpec.setTargetColumnComments(column.getColumnComments());
                mapSpec.setTargetColumnAlias(column.getColumnAlias());
                mapSpec.setTargetNaturalKeyFlag(column.isNaturalKeyFlag());
                mapSpec.setTargetPrimaryKeyFlag(column.isPrimaryKeyFlag());
                mapSpec.setTargetSDIDescription(column.getSDIDescription());
            }
        } catch (Exception e) {
            LOGGER.error("Exception Occured at MetaDataSync inside addTargetColumnsDetails()>>", e);
        }

    }

    private static void addSourceColumnsDetails(MappingSpecificationRow mapSPecs, String Sourcetablename, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, Map<String, Map<Integer, SMColumn>> ColsDetails, List<String> envMap, SystemManagerUtil smutill) {
        try {
            if (Sourcetablename.split("\n").length > 1) {
                String sourceTable = Sourcetablename.split("\n")[0];
                String sourceColumn = mapSPecs.getSourceColumnName().split("\n")[0];
                addSourceColumnsDetails(mapSPecs, sourceTable, sourceColumn, sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
            } else {
                addSourceColumnsDetails(mapSPecs, Sourcetablename, mapSPecs.getSourceColumnName(), sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
            }
        } catch (Exception e) {
            LOGGER.error("Exception Occured at MetaDataSync inside addSourceColumnsDetails()>>", e);
        }

    }

    public static List<String> getappendtargetTables(String[] targetTablename, String MapName) {
        List<String> tgtTables = new LinkedList<>();
        try {
            for (String targetTab : targetTablename) {
                if (targetTab.contains(".")) {
                    if (targetTab.split("\\.").length > 2) {
                        targetTab = targetTab.split("\\.")[targetTab.split("\\.").length - 2] + "." + targetTab.split("\\.")[targetTab.split("\\.").length - 1];
                    }
                }
                
                tgtTables.add(targetTab);
            }
        } catch (Exception e) {
            LOGGER.error("Exception Occured at MetaDataSync inside getappendtargetTables()>>", e);
        }
        return tgtTables;
    }

    private static void addTargetColumnsDetails(MappingSpecificationRow mapSPecs, String targettablename, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, Map<String, Map<Integer, SMColumn>> ColsDetails, List<String> envMap, SystemManagerUtil smutill) {
        try {
            if (targettablename.split("\n").length > 1) {
                String targetTable = targettablename.split("\n")[0];
                String targetColumn = mapSPecs.getTargetColumnName().split("\n")[0];
                addTargetColumnsDetails(mapSPecs, targetTable, targetColumn, sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
            } else {
                addTargetColumnsDetails(mapSPecs, targettablename, mapSPecs.getTargetColumnName(), sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
            }
        } catch (Exception e) {
            LOGGER.error("Exception Occured at MetaDataSync inside addSourceColumnsDetails()>>", e);
        }

    }

    private static void addSourceColumnsDetails(MappingSpecificationRow mapSpec, String columnName, String tableName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, Map<String, Map<Integer, SMColumn>> ColsDetails, List<String> sysEnvDetails, SystemManagerUtil smUtil) {

        try {
            SMColumn column = getColDetails(columnName, tableName, sysEnvTableDetails, tableColDetails, ColsDetails, sysEnvDetails, smUtil);
            if (column != null) {
                mapSpec.setSourceColumnDatatype(column.getColumnDatatype());
                mapSpec.setSourceColumnLength(StringUtils.isNumeric(column.getColumnLength()) ? Integer.parseInt(column.getColumnLength()) : 0);
                mapSpec.setSourceColumnPrecision(StringUtils.isNumeric(column.getColumnPrecision()) ? Integer.parseInt(column.getColumnPrecision()) : 0);
                mapSpec.setSourceColumnDBDefaultValue(column.getColumnDBDefaultValue());
                mapSpec.setSourceColumnDefinition(column.getColumnDefinition());
                mapSpec.setSourceColumnIdentityFlag(column.isColumnIdentityFlag());
                mapSpec.setSourceColumnNullableFlag(column.isColumnNullableFlag());
                mapSpec.setSourceColumnClass(column.getColumnClass());
                mapSpec.setSourceColumnComments(column.getColumnComments());
                mapSpec.setSourceColumnAlias(column.getColumnAlias());
                mapSpec.setSourceNaturalKeyFlag(column.isNaturalKeyFlag());
                mapSpec.setSourcePrimaryKeyFlag(column.isPrimaryKeyFlag());
                mapSpec.setSourceMaximumValue(StringUtils.isNumeric(column.getMaximumValue()) ? Integer.parseInt(column.getMaximumValue()) : 0);
                mapSpec.setSourceMinimumValue(StringUtils.isNumeric(column.getMinimumValue()) ? Integer.parseInt(column.getMinimumValue()) : 0);
                mapSpec.setSourceSDIDescription(column.getSDIDescription());
            }
        } catch (Exception e) {
            LOGGER.error("Exception Occured at MetaDataSync inside addSourceColumnsDetails()>>", e);
        }

    }

    public static void targetNameSet(String Targettablename, List<String> envMap, MappingSpecificationRow mapSPecs, String mapName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill, String dataflowName, Set<String> tableNotFound,String storeProcMapName) {

        if (Targettablename.split("\n").length > 1) {
            String[] tgttablecolumn = Targettablename.split("\n");
            List<String> tgtsystem = getTargetSystem(tgttablecolumn, envMap, mapSPecs, sysEnvTableDetails, tableColDetails, smutill, tableNotFound,dataflowName,storeProcMapName);
            List<String> tgteenv = getTargetEnv(tgttablecolumn, envMap, mapSPecs, sysEnvTableDetails, tableColDetails, smutill, dataflowName, tableNotFound,storeProcMapName);
            List<String> tgttab = getappendtargetTables(tgttablecolumn, mapName);
            String tgtSystem = StringUtils.join(tgtsystem, "\n");
            String tgtEnv = StringUtils.join(tgteenv, "\n");
            String Tgttab = StringUtils.join(tgttab, "\n");
            mapSPecs.setTargetSystemName(tgtSystem);
            mapSPecs.setTargetSystemEnvironmentName(tgtEnv);
            mapSPecs.setTargetTableName(Tgttab);
        } else {
            if (Targettablename.toUpperCase().contains("RESULT_OF_") || Targettablename.toUpperCase().contains("INSERT-SELECT") || Targettablename.toUpperCase().contains("UPDATE-SELECT")
                    || Targettablename.toUpperCase().startsWith("RS-") || Targettablename.toUpperCase().startsWith("#") || Targettablename.toUpperCase().startsWith("CTE-") || Targettablename.equalsIgnoreCase("storeProcMapName") || tableNotFound.contains(Targettablename)) {
                mapSPecs.setTargetSystemName("SSIS SYSTEM");
                if (StringUtils.isNotBlank(dataflowName)) {
                    mapSPecs.setTargetSystemEnvironmentName(dataflowName);
                } else {
                   if (!mapSPecs.getTargetSystemEnvironmentName().equalsIgnoreCase(mapName)) {
                    mapSPecs.setTargetSystemEnvironmentName("SSIS ENVIRONMENT");
                   }
                }
            } else {
                String tableName = Targettablename;
                if (mapSPecs.getTargetTableName().split("\n").length == 1 && mapSPecs.getTargetTableName().split("\\.").length >= 4) {
                    if (!sysEnvTableDetails.containsKey(mapSPecs.getTargetTableName())) {
                        updateTableDetails(envMap, Targettablename, smutill, sysEnvTableDetails, tableColDetails, mapSPecs.getTargetTableName());
                    }
                    if ((!sysEnvTableDetails.containsKey(mapSPecs.getTargetTableName())) && (!sysEnvTableDetails.containsKey(tableName))) {
                        updateTableDetails(envMap, tableName, smutill, sysEnvTableDetails, tableColDetails);
                    }
                    if (sysEnvTableDetails.containsKey(mapSPecs.getTargetTableName())) {
                        tableName = mapSPecs.getTargetTableName();
                    }
                }
                if (!sysEnvTableDetails.containsKey(tableName) && (!tableNotFound.contains(Targettablename))) {
                    updateTableDetails(envMap, tableName, smutill, sysEnvTableDetails, tableColDetails);
                }
                if (sysEnvTableDetails.containsKey(Targettablename)) {
                    String targetenvSys = sysEnvTableDetails.get(Targettablename);
                    String SystemName = targetenvSys.split("###")[0];
                    mapSPecs.setTargetSystemName(SystemName);
                    String environmentName = targetenvSys.split("###")[1];
                    mapSPecs.setTargetSystemEnvironmentName(environmentName);
                } //For datflow query Mappings physical tables definety having dataflowName there we need to set Sqlserver,sql 
                else {
                    mapSPecs.setTargetSystemName("SSIS SYSTEM");
                    if (StringUtils.isNotBlank(dataflowName)) {
                        mapSPecs.setTargetSystemEnvironmentName(dataflowName);
                    } else {
                        if (!mapSPecs.getTargetSystemEnvironmentName().equalsIgnoreCase(mapName)) {
                        mapSPecs.setTargetSystemEnvironmentName("SSIS ENVIRONMENT");
                        }
                    }
                    tableNotFound.add(Targettablename);
                }
            }

            mapSPecs.setTargetTableName(Targettablename);
        }

    }

    public static List<String> getappendSourceTables(String[] sourcetablename, String MapName) {
        List<String> sourcTables = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if (sourceTab.contains(".")) {
                    if (sourceTab.split("\\.").length > 2) {
                        sourceTab = sourceTab.split("\\.")[sourceTab.split("\\.").length - 2] + "." + sourceTab.split("\\.")[sourceTab.split("\\.").length - 1];
                    }
                }
                sourcTables.add(sourceTab);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourcTables;
    }

    public static List<String> getSourceSystem(String[] sourcetablename, List<String> envMap, MappingSpecificationRow mapSPecs, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill, Set<String> tableNotFound, String dataFlowName,String storeProcMapName) {
        List<String> sourcesystem = new LinkedList<>();
        boolean tableexistance=true;
        try {
            String SystemName = "";
            for (String sourceTab : sourcetablename) {
                boolean addingTable=true;
                if(sourceTab.toUpperCase().trim().contains("RESULT_OF_") || sourceTab.toUpperCase().trim().contains("INSERT-SELECT") || sourceTab.toUpperCase().trim().contains("UPDATE-SELECT")
                    || sourceTab.toUpperCase().trim().startsWith("RS-") || sourceTab.toUpperCase().trim().startsWith("#") || sourceTab.toUpperCase().trim().startsWith("CTE-") || sourceTab.trim().equalsIgnoreCase(storeProcMapName))
                {
                    SystemName = "SSIS SYSTEM";
                    if (StringUtils.isNotBlank(dataFlowName)) {
                        SystemName = "SSIS SYSTEM";
                    }
                    sourcesystem.add(SystemName);
                    addingTable=false;
                }
               else if (tableNotFound.contains(sourceTab) && addingTable) {
                    if (StringUtils.isNotBlank(dataFlowName)) {
                        SystemName = "MSSQL";
                    } else {
                        SystemName = "SSIS SYSTEM";
                    }
                    sourcesystem.add(SystemName);
                } else {
                     if(addingTable){
                    if (!sysEnvTableDetails.containsKey(sourceTab)) {
                        updateTableDetails(envMap, sourceTab, smutill, sysEnvTableDetails, tableColDetails);
                    }
                    if (sysEnvTableDetails.containsKey(sourceTab)) {
                        String sourceenvSys = sysEnvTableDetails.get(sourceTab);
                        sourcesystem.add(sourceenvSys.split("###")[0]);
                    } else {
                        if (StringUtils.isNotBlank(dataFlowName)) {
                            SystemName = "MSSQL";
                        } else {
                            SystemName = "SSIS SYSTEM";
                        }
                        sourcesystem.add(SystemName);
                        tableNotFound.add(sourceTab);
                    }
                }
               }
            }
        } catch (Exception e) {
            LOGGER.error("Exception Occured at MetaDataSync inside getSourceSystem()>>", e);
        }
        return sourcesystem;
    }

    public static List<String> getTargetSystem(String[] sourcetablename, List<String> envMap, MappingSpecificationRow mapSPecs, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill, Set<String> tableNotFound,String dataFlowName,String storeprocmapName) {
        List<String> sourcesystem = new LinkedList<>();
        try {
            String SystemName = "";
            for (String sourceTab : sourcetablename) {
                if(sourceTab.toUpperCase().contains("RESULT_OF_") || sourceTab.toUpperCase().contains("INSERT-SELECT") || sourceTab.toUpperCase().contains("UPDATE-SELECT")
                    || sourceTab.toUpperCase().startsWith("RS-") || sourceTab.toUpperCase().startsWith("#") || sourceTab.toUpperCase().startsWith("CTE-") || sourceTab.equalsIgnoreCase(storeprocmapName) || tableNotFound.contains(sourceTab))
                {
                    SystemName = "SSIS SYSTEM";
                    sourcesystem.add(SystemName);
                }
                 else {
                    if (!sysEnvTableDetails.containsKey(sourceTab)) {
                        updateTableDetails(envMap, sourceTab, smutill, sysEnvTableDetails, tableColDetails);
                    }
                    if (sysEnvTableDetails.containsKey(sourceTab)) {
                        String sourceenvSys = sysEnvTableDetails.get(sourceTab);
                        sourcesystem.add(sourceenvSys.split("###")[0]);
                    } else {
                         SystemName = "SSIS SYSTEM";
                        sourcesystem.add(SystemName);
                        tableNotFound.add(sourceTab);
                    }
                }

            }
        } catch (Exception e) {
            LOGGER.error("Exception Occured at MetaDataSync inside getTargetSystem()>>", e);
        }
        return sourcesystem;
    }

    public static List<String> getSourceEnv(String[] sourcetablename, List<String> envMap, MappingSpecificationRow mapSPecs, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill, String dataflowName, Set<String> tableNotFound,String storeProcMapName) {
        List<String> sourceenv = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                boolean existTable=true;
                if(sourceTab.trim().toUpperCase().contains("RESULT_OF_") || sourceTab.trim().toUpperCase().contains("INSERT-SELECT") || sourceTab.trim().toUpperCase().contains("UPDATE-SELECT")
                    || sourceTab.trim().toUpperCase().startsWith("RS-") || sourceTab.trim().toUpperCase().startsWith("#") || sourceTab.trim().toUpperCase().startsWith("CTE-") || sourceTab.trim().equalsIgnoreCase(storeProcMapName))
                {
                    String envName = "SSIS ENVIRONMENT";
                    if (StringUtils.isNotBlank(dataflowName)) {
                          envName = dataflowName;
                    }
                    sourceenv.add(envName);
                    existTable=false;
                }
               else if (tableNotFound.contains(sourceTab) && existTable) {
                    String envName = "SSIS ENVIRONMENT";
                    if (StringUtils.isNotBlank(dataflowName)) {
                        envName = "SQL";
                    }
                    sourceenv.add(envName);
                } else {
                      if(existTable){
                    if (!sysEnvTableDetails.containsKey(sourceTab)) {
                        updateTableDetails(envMap, sourceTab, smutill, sysEnvTableDetails, tableColDetails);
                    }
                    if (sysEnvTableDetails.containsKey(sourceTab)) {
                        String sourceenvSys = sysEnvTableDetails.get(sourceTab);
                        sourceenv.add(sourceenvSys.split("###")[1]);
                    } else {
                        String envName = "SSIS ENVIRONMENT";
                        if (StringUtils.isNotBlank(dataflowName)) {
                            envName = "SQL";
                        }
                        sourceenv.add(envName);
                        tableNotFound.add(sourceTab);
                    }
                }
               }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceenv;
    }

    public static List<String> getTargetEnv(String[] sourcetablename, List<String> envMap, MappingSpecificationRow mapSPecs, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill, String dataFlowName, Set<String> tableNotFound,String storeProcMapName) {
        List<String> sourceenv = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if(sourceTab.toUpperCase().contains("RESULT_OF_") || sourceTab.toUpperCase().contains("INSERT-SELECT") || sourceTab.toUpperCase().contains("UPDATE-SELECT")
                    || sourceTab.toUpperCase().startsWith("RS-") || sourceTab.toUpperCase().startsWith("#") || sourceTab.toUpperCase().startsWith("CTE-") || tableNotFound.contains(sourceTab) || sourceTab.equalsIgnoreCase(storeProcMapName))
                {
                    String envName = "SSIS ENVIRONMENT";
                    if (StringUtils.isNotBlank(dataFlowName)) {
                          envName = dataFlowName;
                    }
                    sourceenv.add(envName);
                }
                 else {
                    if (!sysEnvTableDetails.containsKey(sourceTab)) {
                        updateTableDetails(envMap, sourceTab, smutill, sysEnvTableDetails, tableColDetails);
                    }
                    if (sysEnvTableDetails.containsKey(sourceTab)) {
                        String sourceenvSys = sysEnvTableDetails.get(sourceTab);
                        sourceenv.add(sourceenvSys.split("###")[1]);
                    } else {
                        String envName = "SSIS ENVIRONMENT";
                        if (StringUtils.isNotBlank(dataFlowName)) {
                            envName = dataFlowName;
                        }
                        sourceenv.add(envName);
                        tableNotFound.add(sourceTab);
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceenv;
    }

    private static void updateTableDetails(List<String> sysEnvDetails, String tableName, SystemManagerUtil smUtil, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails) {
        Map<String, Integer> cols = new HashMap<>();
        for (int i = 0; i < sysEnvDetails.size(); i++) {
            String[] sysEnvDetail = sysEnvDetails.get(i).split("###");
            try {
                int tableId = smUtil.getTableId(sysEnvDetail[0], sysEnvDetail[1], tableName);
                if (tableId != -1) {
                    List<SMColumn> colList = smUtil.getColumns(tableId);
                    colList.stream().forEach(col -> {
                        cols.put(col.getColumnName(), col.getColumnId());
                    });
                    tableColDetails.put(tableName, cols);
                    sysEnvTableDetails.put(tableName, sysEnvDetails.get(i) + "###" + tableId);
                    break;
                }

            } catch (Exception e) {
                LOGGER.error("Exception Occured at MetaDataSync inside updateTableDetails()>>", e);
            }

        }
    }

    public static void sourceNamesSet(String Sourcetablename, List<String> envMap, MappingSpecificationRow mapSPecs, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill, String mapName, String dataflowName, Set<String> tableNotFound,String storeProcMapName) {
        if (Sourcetablename.split("\n").length > 1) {
          //  StringBuilder tables = new StringBuilder();
            String tables="";
            List<String> sourcetableslist = new LinkedList<>();
            String[] sourcetablecolumn = Sourcetablename.split("\n");
            for (String sourcetables : sourcetablecolumn) {
                if (sourcetables.split("\\.").length >= 3) {
                    sourcetables = sourcetables.split("\\.")[sourcetables.split("\\.").length - 2] + "." + sourcetables.split("\\.")[sourcetables.split("\\.").length - 1];
                }
                sourcetableslist.add(sourcetables);
                tables = StringUtils.join(sourcetableslist, "\n");
            }
            String srctabcolumns[] = tables.split("\n");
            List<String> sourcesystem = getSourceSystem(srctabcolumns, envMap, mapSPecs, sysEnvTableDetails, tableColDetails, smutill, tableNotFound, dataflowName,storeProcMapName);
            List<String> sourceenv = getSourceEnv(srctabcolumns, envMap, mapSPecs, sysEnvTableDetails, tableColDetails, smutill, dataflowName, tableNotFound,storeProcMapName);
            List<String> sourceTabc = getappendSourceTables(sourcetablecolumn, mapName);
            String Sourcetables = StringUtils.join(sourceTabc, "\n");
            String sourceSystem = StringUtils.join(sourcesystem,"\n");
            String sourceEnv = StringUtils.join(sourceenv, "\n");
            mapSPecs.setSourceTableName(Sourcetables.trim());
            mapSPecs.setSourceSystemEnvironmentName(sourceEnv.trim());
            mapSPecs.setSourceSystemName(sourceSystem.trim());
        } else {
            if (Sourcetablename.toUpperCase().contains("RESULT_OF_") || Sourcetablename.toUpperCase().contains("INSERT-SELECT") || Sourcetablename.toUpperCase().contains("UPDATE-SELECT")
                    || Sourcetablename.toUpperCase().startsWith("RS-") || Sourcetablename.toUpperCase().startsWith("#") || Sourcetablename.toUpperCase().startsWith("CTE-") || Sourcetablename.equalsIgnoreCase(storeProcMapName)) {
                mapSPecs.setSourceSystemName("SSIS SYSTEM");
                if (StringUtils.isNotBlank(dataflowName)) {
                    mapSPecs.setSourceSystemEnvironmentName(dataflowName);
                } else {
                    if (!mapSPecs.getSourceSystemEnvironmentName().equalsIgnoreCase(mapName)) {
                        mapSPecs.setSourceSystemEnvironmentName("SSIS ENVIRONMENT");
                    }
                }
            } else {
                String tableName = Sourcetablename;
                if (mapSPecs.getSourceTableName().split("\n").length == 1 && mapSPecs.getSourceTableName().split("\\.").length >= 4) {
                    if (!sysEnvTableDetails.containsKey(mapSPecs.getSourceTableName())) {
                        updateTableDetails(envMap, Sourcetablename, smutill, sysEnvTableDetails, tableColDetails, mapSPecs.getSourceTableName());
                    }
                    if ((!sysEnvTableDetails.containsKey(mapSPecs.getSourceTableName())) && (!sysEnvTableDetails.containsKey(tableName))) {
                        updateTableDetails(envMap, tableName, smutill, sysEnvTableDetails, tableColDetails);
                    }
                    if (sysEnvTableDetails.containsKey(mapSPecs.getSourceTableName())) {
                        tableName = mapSPecs.getSourceTableName();
                    }
                }
                if (!sysEnvTableDetails.containsKey(tableName) && (!tableNotFound.contains(tableName))) {
                    updateTableDetails(envMap, tableName, smutill, sysEnvTableDetails, tableColDetails);
                }
                if (sysEnvTableDetails.containsKey(tableName)) {
                    String sourceenvSys = sysEnvTableDetails.get(Sourcetablename);
                    String SystemName = sourceenvSys.split("###")[0];
                    mapSPecs.setSourceSystemName(SystemName);
                    String environmentName = sourceenvSys.split("###")[1];
                    mapSPecs.setSourceSystemEnvironmentName(environmentName);

                } else {
                    if (StringUtils.isNotBlank(dataflowName)) {
                        mapSPecs.setSourceSystemName("MSSQL");
                        mapSPecs.setSourceSystemEnvironmentName("SQL");
                    } else {
                        if (!mapSPecs.getSourceSystemEnvironmentName().equalsIgnoreCase(mapName)) {
                            mapSPecs.setSourceSystemName("SSIS SYSTEM");
                            mapSPecs.setSourceSystemEnvironmentName("SSIS ENVIRONMENT");
                        } else {
                            mapSPecs.setSourceSystemName("SSIS SYSTEM");
                            // mapSPecs.setSourceSystemEnvironmentName("SSIS ENVIRONMENT");
                        }
                    }
                    tableNotFound.add(Sourcetablename);
                }
            }

            mapSPecs.setSourceTableName(Sourcetablename.trim());
        }
    }

    public static void dolookupwithstarcolumns(List<MappingSpecificationRow> mapspeclist) {
        try {
            ArrayList<MappingSpecificationRow> speclist = new ArrayList<>();
            for (MappingSpecificationRow mappingSpecificationRow : mapspeclist) {

                String sourcetableName = mappingSpecificationRow.getSourceTableName();
                String srcsysName = mappingSpecificationRow.getSourceSystemName();
                String srcenvName = mappingSpecificationRow.getSourceSystemEnvironmentName();
                String tgtsysName = mappingSpecificationRow.getTargetSystemName();
                String tgtenvName = mappingSpecificationRow.getTargetSystemEnvironmentName();
                String tgttabName = mappingSpecificationRow.getTargetTableName();

                String columnName = mappingSpecificationRow.getSourceColumnName();
                if (columnName.contains("*")) {
                    List<String> columns = getColumnsfromTableName(mapspeclist, sourcetableName);
                    for (String column : columns) {
                        MappingSpecificationRow specrow = new MappingSpecificationRow();
                        specrow.setSourceSystemName(srcsysName);
                        specrow.setSourceSystemEnvironmentName(srcenvName);
                        specrow.setTargetSystemName(tgtsysName);
                        specrow.setTargetSystemEnvironmentName(tgtenvName);
                        specrow.setSourceTableName(sourcetableName);

                        specrow.setTargetTableName(tgttabName);

                        specrow.setTargetColumnName(column);

                        specrow.setSourceColumnName(column);
                        speclist.add(specrow);

                    }

                }

            }
            mapspeclist.addAll(speclist);
        } catch (Exception e) {
            LOGGER.error("Exception Occured at MetaDataSync inside dolookupwithstarcolumns()>>", e);
        }

    }

    public static List<String> getColumnsfromTableName(List<MappingSpecificationRow> mapspeclist, String tableName) {
        List<String> columns = new LinkedList<>();
        try {

            for (MappingSpecificationRow mappingSpecificationRow : mapspeclist) {

                String sourceTableName = mappingSpecificationRow.getSourceTableName();

                if (tableName.equalsIgnoreCase(sourceTableName)) {
                    columns.add(mappingSpecificationRow.getSourceColumnName());

                }

            }

        } catch (Exception e) {

            LOGGER.error("Exception Occured at MetaDataSync inside getColumnsfromTableName()>>", e);
        }
        return columns;
    }

    public static void changeinsertselect(List<MappingSpecificationRow> mapspeclist) {
        try {
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                String sourceTableName = row.getSourceTableName();
                String columnName = row.getSourceColumnName();

                if (sourceTableName.toUpperCase().contains("INSERT")) {
                    if (!checkinserttarget(mapspeclist, sourceTableName, columnName)) {
                        iter.remove();
                    }
                }

            }

        } catch (Exception e) {

            LOGGER.error("Exception Occured at MetaDataSync inside changeinsertselect()>>", e);
        }

    }

    public static boolean checkinserttarget(List<MappingSpecificationRow> mapspeclist, String tableName, String ColumnName) {

        try {
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                String targetTableName = row.getTargetTableName();
                String targetColumnName = row.getTargetColumnName();

                if (targetTableName.equalsIgnoreCase(tableName) && targetColumnName.equalsIgnoreCase(ColumnName)) {

                    return true;
                }
            }

        } catch (Exception e) {

            LOGGER.error("Exception Occured at MetaDataSync inside checkinserttarget()>>", e);
        }
        return false;
    }

    public static void appendHashforTable(List<MappingSpecificationRow> mapspeclist) {
        for (MappingSpecificationRow mappingSpecificationRow : mapspeclist) {
            if (mappingSpecificationRow.getSourceTableName().startsWith("#")) {
                String ashsrctabName = mappingSpecificationRow.getSourceTableName();
                String srctabName = ashsrctabName.split("#")[1];

                for (MappingSpecificationRow srcmappingSpecificationRow : mapspeclist) {
                    if (srctabName.equalsIgnoreCase(srcmappingSpecificationRow.getSourceTableName())) {
                        srcmappingSpecificationRow.setSourceTableName(ashsrctabName);
                    }
                }
            }
            if (mappingSpecificationRow.getTargetTableName().startsWith("#")) {
                String ashtgttabName = mappingSpecificationRow.getTargetTableName();
                String tgttabName = ashtgttabName.split("#")[1];
                for (MappingSpecificationRow tgtMappingspecificationRow : mapspeclist) {
                    if (tgttabName.equalsIgnoreCase(tgtMappingspecificationRow.getTargetTableName())) {
                        tgtMappingspecificationRow.setTargetTableName(ashtgttabName);
                    }
                }
            }

        }
    }

    private static void removebraces(List<MappingSpecificationRow> mappingspec) {

        try {
            Iterator<MappingSpecificationRow> iter = mappingspec.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();

                if (row.getSourceTableName().contains("[") || row.getSourceTableName().contains("]")) {

                    row.setSourceTableName(row.getSourceTableName().replace("[", "").replace("]", ""));
                }
                if (row.getSourceColumnName().contains("[") || row.getSourceColumnName().contains("]")) {

                    row.setSourceColumnName(row.getSourceColumnName().replace("[", "").replace("]", ""));
                }
                if (row.getTargetTableName().contains("[") || row.getTargetTableName().contains("]")) {

                    row.setTargetTableName(row.getSourceTableName().replace("[", "").replace("]", ""));
                }
                if (row.getTargetColumnName().contains("[") || row.getTargetColumnName().contains("]")) {

                    row.setTargetColumnName(row.getTargetColumnName().replace("[", "").replace("]", ""));
                }
            }

        } catch (Exception e) {
            LOGGER.error("Exception Occured at MetaDataSync inside removebraces()>>", e);
        }

    }

    private static void changeresultofTarget(List<MappingSpecificationRow> mapspeclist) {
        try {
            Set<String> targettabset = new LinkedHashSet();
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                StringBuilder sourcetabsb = new StringBuilder();
                StringBuilder targettabsb = new StringBuilder();
                String targetTabName = row.getTargetTableName().replace("[", "").replace("]", "");
                String SourceTableName = row.getSourceTableName().replace("[", "").replace("]", "");
                String targetColumnName = row.getTargetColumnName().replace("[", "").replace("]", "");
                String sourceColumnName = row.getSourceColumnName().replace("[", "").replace("]", "");

                if (!sourcetabsb.toString().isEmpty()) {
                    row.setSourceTableName(sourcetabsb.toString().trim());
                } else {
                    row.setSourceTableName(SourceTableName.trim());
                }
                if (!targettabsb.toString().isEmpty()) {
                    row.setTargetTableName(targettabsb.toString().trim());
                } else {
                    row.setTargetTableName(targetTabName);
                }
                row.setSourceColumnName(sourceColumnName);
                row.setTargetColumnName(targetColumnName);
            }
        } catch (Exception e) {
            LOGGER.error("Exception Occured at MetaDataSync inside changeresultofTarget()>>", e);
        }
    }

    public static void addSchemaNameforOrphantables(List<MappingSpecificationRow> mapspeclist) {

        for (MappingSpecificationRow mappingSpecificationRow : mapspeclist) {
            String srcschemaName = "";
            String tgtschemaName = "";
            if (!mappingSpecificationRow.getSourceTableName().trim().contains("\\.") && !mappingSpecificationRow.getSourceTableName().toUpperCase().contains("INSERT-SELECT") && !mappingSpecificationRow.getSourceTableName().toUpperCase().contains("UPDATE-SELECT") && !mappingSpecificationRow.getSourceTableName().toUpperCase().contains("RESULT_OF") && !mappingSpecificationRow.getSourceTableName().toUpperCase().startsWith("#") && !mappingSpecificationRow.getSourceTableName().trim().toUpperCase().startsWith("RS-")) {
                String srctabName = mappingSpecificationRow.getSourceTableName().trim();

                for (MappingSpecificationRow mappingSpecificationRow1 : mapspeclist) {
                    String dotsrctabName = mappingSpecificationRow1.getSourceTableName().trim();
                    if (dotsrctabName.split("\\.").length >= 2) {

                        String srcTabName = dotsrctabName.split("\\.")[dotsrctabName.split("\\.").length - 1];
                        if (srctabName.equalsIgnoreCase(srcTabName)) {
                            srcschemaName = dotsrctabName.split("\\.")[dotsrctabName.split("\\.").length - 2];
                            mappingSpecificationRow.setSourceTableName(srcschemaName + "." + srctabName);
                            break;
                        }

                    }
                    String dottgtabName = mappingSpecificationRow1.getTargetTableName().trim();
                    if (dottgtabName.split("\\.").length >= 2) {

                        String tgtTabName = dottgtabName.split("\\.")[dottgtabName.split("\\.").length - 1];
                        if (srctabName.equalsIgnoreCase(tgtTabName)) {
                            srcschemaName = dottgtabName.split("\\.")[dottgtabName.split("\\.").length - 2];
                            mappingSpecificationRow.setSourceTableName(srcschemaName + "." + srctabName);
                            break;
                        }

                    }

                }

            }
            if (!mappingSpecificationRow.getTargetTableName().trim().contains("\\.") && !mappingSpecificationRow.getTargetTableName().trim().toUpperCase().contains("INSERT-SELECT") && !mappingSpecificationRow.getTargetTableName().trim().toUpperCase().contains("UPDATE-SELECT") && !mappingSpecificationRow.getTargetTableName().trim().toUpperCase().contains("RESULT_OF") && !mappingSpecificationRow.getTargetTableName().toUpperCase().startsWith("#") && !mappingSpecificationRow.getTargetTableName().trim().toUpperCase().startsWith("RS-")) {
                String tgttabName = mappingSpecificationRow.getTargetTableName().trim();
                for (MappingSpecificationRow tgtmappingSpecificationRow1 : mapspeclist) {
                    String dotsrctabName = tgtmappingSpecificationRow1.getSourceTableName().trim();
                    if (dotsrctabName.split("\\.").length >= 2) {
                        String srcTabName = dotsrctabName.split("\\.")[dotsrctabName.split("\\.").length - 1];
                        if (tgttabName.equalsIgnoreCase(srcTabName)) {
                            tgtschemaName = dotsrctabName.split("\\.")[dotsrctabName.split("\\.").length - 2];
                            mappingSpecificationRow.setTargetTableName(tgtschemaName + "." + tgttabName);
                            break;
                        }
                    }
                    String dotTgttabName = tgtmappingSpecificationRow1.getTargetTableName().trim();
                    if (dotTgttabName.split("\\.").length >= 2) {
                        String tgtTabName = dotTgttabName.split("\\.")[dotTgttabName.split("\\.").length - 1];
                        if (tgttabName.equalsIgnoreCase(tgtTabName)) {
                            tgtschemaName = dotTgttabName.split("\\.")[dotTgttabName.split("\\.").length - 2];
                            mappingSpecificationRow.setTargetTableName(tgtschemaName + "." + tgttabName);
                            break;
                        }
                    }
                }

            }

        }
    }

    private static void updateTableDetails(List<String> sysEnvDetails, String tableName, SystemManagerUtil smUtil, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, String sysEnvTableName) {
        Map<String, Integer> cols = new HashMap<>();
        String sysEnv = null;
        Boolean entry = true;
        String hostName = "";
        String dbName = "";
        int tableId = -1;
        if (StringUtils.isBlank(sysEnvTableName)) {
            return;
        }
        if (sysEnvTableName.split("\\.").length >= 4) {
            String[] Sourcetablename = sysEnvTableName.split("\\.");
            // srctableName=Sourcetablename[Sourcetablename.length - 2]+Sourcetablename[Sourcetablename.length - 1];
            dbName = Sourcetablename[Sourcetablename.length - 3];
            for (int t = 0; t < Sourcetablename.length - 3; t++) {
                if (t != 0) {
                    hostName = hostName + ".";
                }
                hostName = hostName + Sourcetablename[t];
            }
        }
        for (int i = 0; i < sysEnvDetails.size(); i++) {
            String[] sysEnvDetail = sysEnvDetails.get(i).split("###");
            try {
                //    SMTable smTab = smUtil.getTable(tableId);
                //  SMEnvironment smEnv = smTab.getEnvironment();
                SMEnvironment smEnv = smUtil.getEnvironment(Integer.parseInt(sysEnvDetail[2]), false);
                if (smEnv != null) {
                    if (StringUtils.isNotBlank(hostName) && StringUtils.isNotBlank(dbName)) {
                        if (hostName.toUpperCase().equals(smEnv.getDatabaseIPAddress().toUpperCase()) && dbName.toUpperCase().equals(smEnv.getDatabaseName().toUpperCase())) {
                            sysEnv = sysEnvDetails.get(i);
                            tableId = smUtil.getTableId(sysEnvDetail[0], sysEnvDetail[1], tableName);
                            if (tableId != -1) {
                                break;
                            }
                        } else {
                            if (entry) {
                                tableId = smUtil.getTableId(sysEnvDetail[0], sysEnvDetail[1], tableName);
                                if (tableId != -1) {
                                    sysEnv = sysEnvDetails.get(i);
                                    entry = false;
                                }
                            }

                        }
                    }
                }

            } catch (Exception e) {
                LOGGER.error("Exception Occured at MetaDataSync inside updateTableDetails()>>", e);
            }
        }

        try {
            if (StringUtils.isNotEmpty(sysEnv)) {
                String[] sysEnvDetail = sysEnv.split("###");
                //    tableId = smUtil.getTableId(sysEnvDetail[0], sysEnvDetail[1], tableName);
                if (tableId != -1) {
                    List<SMColumn> colList = smUtil.getColumns(tableId);
                    colList.stream().forEach(col -> {
                        cols.put(col.getColumnName(), col.getColumnId());
                    });
                    tableColDetails.put(sysEnvTableName.toUpperCase(), cols);
                    tableColDetails.put(tableName, cols);
                    sysEnvTableDetails.put(tableName.toUpperCase(), sysEnv + "###" + tableId);
                    sysEnvTableDetails.put(sysEnvTableName.toUpperCase(), sysEnv + "###" + tableId);
                }
            }

        } catch (Exception e) {
            LOGGER.error("Exception Occured at MetaDataSync inside updateTableDetails()>>", e);
        }
    }

    private static SMColumn getColDetails(String columnName, String tableName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, Map<String, Map<Integer, SMColumn>> ColsDetails, List<String> sysEnvDetails, SystemManagerUtil smUtil) {
        if (!tableColDetails.containsKey(tableName)) {
            updateTableDetails(sysEnvDetails, tableName, smUtil, sysEnvTableDetails, tableColDetails);
        }
        if (tableColDetails.containsKey(tableName)) {
            Map<String, Integer> colDetails = tableColDetails.get(tableName);
            if (colDetails.containsKey(columnName.trim())) {
                Integer colId = colDetails.get(columnName.trim());
                if (!ColsDetails.containsKey(tableName)) {
                    Map<Integer, SMColumn> columnMap = new HashMap<>();
                    ColsDetails.put(tableName, columnMap);
                }
                if (ColsDetails.containsKey(tableName)) {
                    Map<Integer, SMColumn> columnMap = ColsDetails.get(tableName);
                    if (!columnMap.containsKey(colId)) {
                        try {
                            SMColumn column = smUtil.getColumn(colId);
                            columnMap.put(colId, column);
                        } catch (Exception e) {
                            columnMap.put(colId, null);
                            LOGGER.error("Exception Occured at MetaDataSync inside updateColDetails()>>", e);
                        }

                    }
                    return columnMap.get(colId);
                }
            }
        }
        return null;
    }

    public static String getdataFlowName(String MapName, ArrayList<MappingSpecificationRow> specificationRows) {
        String dataflowName = "";
        try {
            for (MappingSpecificationRow specificationRow : specificationRows) {
                String tgt_tab_Name = specificationRow.getTargetTableName();
                if (tgt_tab_Name.equalsIgnoreCase(MapName)) {
                    dataflowName = specificationRow.getTargetSystemEnvironmentName();
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataflowName;
    }
   public static String gettingStoreprocMapName(String MapName, ArrayList<MappingSpecificationRow> specificationRows) {
        String spMapName = "";
        String finalspName="";
        String TableName="";
        boolean entryif=true;
        boolean getTable=true;
        try {
            for (MappingSpecificationRow specificationRow : specificationRows) {
                 if(entryif)
                {
                String tgt_tab_Name = specificationRow.getTargetTableName();
                if (tgt_tab_Name.equalsIgnoreCase(MapName))
                {
                    spMapName = specificationRow.getSourceTableName();
                  TableName=gettingFinalspName(spMapName,specificationRows);
                    entryif=false;
                    break;
                }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return TableName;
    }
   public static String gettingFinalspName(String spMapName, ArrayList<MappingSpecificationRow> specificationRows) {
     String finalspName="";
     boolean getTable=true;
       for (MappingSpecificationRow specificationRow : specificationRows) {
           if(getTable) 
           {
           String src_tab_Name=specificationRow.getSourceTableName();
           if(src_tab_Name.toUpperCase().contains("RESULT_OF_") || src_tab_Name.toUpperCase().contains("INSERT-SELECT") || src_tab_Name.toUpperCase().contains("UPDATE-SELECT") || src_tab_Name.toUpperCase().startsWith("RS-"))
           {
           if(src_tab_Name.contains(spMapName))
                {
                    finalspName=spMapName;
                    getTable=false;
                    break;
                }
           }
           }
       }
        
       return finalspName;
   }
   public static void trimtables(List<MappingSpecificationRow> mapspeclist) {
        try {
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                List<String> srclist=new LinkedList<>();
                String sourcetable=row.getSourceTableName();
                String sourcetables[]=sourcetable.split("\n");
                for (String sourcetable1 : sourcetables) {
                     srclist.add(sourcetable1.trim());
                }
             String srctables=StringUtils.join(srclist,"\n");
             row.setSourceTableName(srctables.trim());
             String targettable=row.getTargetTableName();
             List<String> tgtlist=new LinkedList<>();
             String targettables[]=targettable.split("\n");
                for (String targettable1 : targettables) {
                     tgtlist.add(targettable1.trim());
                }
             String tgttables=StringUtils.join(tgtlist,"\n"); 
             row.setTargetTableName(tgttables.trim());
            }
        } catch (Exception e) {
            LOGGER.error("Exception Occured at MetaDataSync inside changeresultofTarget()>>", e);
        }
    }
   public static void removestarcolumspec(List<MappingSpecificationRow> mapspeclist) {
        try {
            Map<String,String> starmap=new LinkedHashMap();
            Map<String,String> normalmap=new LinkedHashMap();
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                if (row.getSourceColumnName().equalsIgnoreCase("*")) {
                   starmap.put(row.getSourceTableName().toUpperCase().trim(),row.getSourceColumnName().toUpperCase().trim());
                }
                else
                {
                    normalmap.put(row.getSourceTableName().toUpperCase().trim(),row.getSourceColumnName().toUpperCase().trim());
                }
                if(row.getTargetTableName().equalsIgnoreCase("")&&row.getTargetColumnName().equalsIgnoreCase("")&&row.getBusinessRule().equalsIgnoreCase(""))
                {
                    iter.remove();
                }
            }
            forRemovingStar(mapspeclist,starmap,normalmap);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static void forRemovingStar(List<MappingSpecificationRow> mapspeclist,Map<String,String>starmap,Map<String,String> normalmap) {
        try {
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row1 = iter.next();
                if (row1.getSourceColumnName().equalsIgnoreCase("*")) {
                    if(normalmap.containsKey(row1.getSourceTableName().toUpperCase()))
                    {
                        if(starmap.containsKey(row1.getSourceTableName().toUpperCase()))
                        {
                            iter.remove();
                        }
                    }
                }
            }
        }
          catch(Exception e)
             {
                e.printStackTrace();
             }
     }
    public static List<MappingSpecificationRow> updatespecrowforstar(List<MappingSpecificationRow> mapspeclist, List<String> envMap, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill) {
        ArrayList<MappingSpecificationRow> rowlist = new ArrayList();
        try {
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                if (row.getSourceColumnName().equalsIgnoreCase("*")) {
                    String sourceTableName = row.getSourceTableName();
                    if (sourceTableName.split("\n").length == 1) {
                    if (sourceTableName.contains(".")) {
                        if (sourceTableName.split("\\.").length > 2) {
                            sourceTableName = sourceTableName.split("\\.")[sourceTableName.split("\\.").length - 2] + "." + sourceTableName.split("\\.")[sourceTableName.split("\\.").length - 1];
                        }
                    }
                }
                    boolean checkingRemoveStarIterator = false;
                    String targetTableName = row.getTargetTableName();
                    Set<String> columnset = new LinkedHashSet();
                    if (tableColDetails.get(sourceTableName) == null) {
                        if (!sourceTableName.toUpperCase().contains("RESULT") && !sourceTableName.toUpperCase().contains("INSERT") && !sourceTableName.toUpperCase().contains("UPDATE") && !sourceTableName.toUpperCase().startsWith("#") && !sourceTableName.toUpperCase().contains("RS-")) {
                            updateTableDetails(envMap, sourceTableName, smutill, sysEnvTableDetails, tableColDetails);
                        }
                    }
                    if (tableColDetails.get(sourceTableName) != null) {
                        columnset = tableColDetails.get(sourceTableName).keySet();
                        checkingRemoveStarIterator = true;
                    }

                    columnset.remove("*");
                    for (String column : columnset) {
                        MappingSpecificationRow row1 = new MappingSpecificationRow();
                        row1.setSourceSystemEnvironmentName(row.getSourceSystemEnvironmentName());
                        row1.setTargetSystemEnvironmentName(row.getTargetSystemEnvironmentName());
                        row1.setSourceSystemName(row.getSourceSystemName());
                        row1.setTargetSystemName(row.getTargetSystemName());
                        row1.setSourceTableName(sourceTableName);
                        row1.setTargetTableName(targetTableName);
                        row1.setSourceColumnName(column);
                        row1.setTargetColumnName(column);
                        rowlist.add(row1);
                    }

                    if (checkingRemoveStarIterator) {
                        iter.remove();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return rowlist;
    }
    public static List<MappingSpecificationRow> settingstarColsofInsert(List<MappingSpecificationRow> mapspeclist) {
        ArrayList<MappingSpecificationRow> rowlist = new ArrayList();
        try {
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                if (row.getSourceColumnName().equalsIgnoreCase("*")) {
                    String sourceTableName = row.getSourceTableName();
                    String targetTableName = row.getTargetTableName();
                    if (sourceTableName.toUpperCase().contains("RESULT") || sourceTableName.toUpperCase().contains("INSERT") || sourceTableName.toUpperCase().contains("UPDATE") || sourceTableName.toUpperCase().startsWith("#") || sourceTableName.toUpperCase().contains("RS-")) {
                        Set<String> insertcolList = gettingInsertselectColumns(sourceTableName, mapspeclist);
                        for (String column : insertcolList) {
                            MappingSpecificationRow row2 = new MappingSpecificationRow();
                            row2.setSourceSystemEnvironmentName(row.getSourceSystemEnvironmentName());
                            row2.setTargetSystemEnvironmentName(row.getTargetSystemEnvironmentName());
                            row2.setSourceSystemName(row.getSourceSystemName());
                            row2.setTargetSystemName(row.getTargetSystemName());
                            row2.setSourceTableName(sourceTableName);
                            row2.setTargetTableName(targetTableName);
                            row2.setSourceColumnName(column);
                            row2.setTargetColumnName(column);
                            rowlist.add(row2);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rowlist;
    }
     public static Set<String> gettingInsertselectColumns(String sourceTableName, List<MappingSpecificationRow> mapspeclist) {
        Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
        Set<String> insertcolList = new HashSet<String>();
        while (iter.hasNext()) {
            MappingSpecificationRow row = iter.next();
            String targetTabName = row.getTargetTableName().replace("[", "").replace("]", "");
            String srceTableName = row.getSourceTableName().replace("[", "").replace("]", "");
            if (sourceTableName.equalsIgnoreCase(targetTabName)) {
                String srcColumnName = row.getSourceColumnName();
                insertcolList.add(srcColumnName);
            }
        }
        return insertcolList;
    }
     
  }
