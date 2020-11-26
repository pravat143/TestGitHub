/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.metadata_zovio_af;

import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.sm.SMColumn;
import com.ads.api.beans.sm.SMEnvironment;
import com.ads.api.beans.sm.SMSystem;
import com.ads.api.beans.sm.SMTable;
import com.ads.api.util.SystemManagerUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 *
 * @author RamPrasad
 */
public class Syncmetadata_Zovio {

    private static final Logger LOGGER = Logger.getLogger(Syncmetadata_Zovio.class);

    public static String metadataSync(List<String> envMap, SystemManagerUtil smutill, String json, String folderName) {
        String jsonvalue = "";
        try {
            Map<String, String> sysEnvTableDetails = new HashMap<>();
            Map<String, Map<String, Integer>> tableColDetails = new HashMap<>();
            Map<String, Map<Integer, SMColumn>> ColsDetails = new HashMap<>();
            ObjectMapper mapper = new ObjectMapper();
            json = json.replace(",\"childNodes\":[]", "");
            List<Mapping> mapObj = (List) mapper.readValue(json, new TypeReference<List<Mapping>>() {
            });
            ArrayList<MappingSpecificationRow> mapSPecsLists = mapObj.get(0).getMappingSpecifications();
            envMap.toString();
            // envMap.toString();
            String mapName = mapObj.get(0).getMappingName();
            String storproc = mapName;
            String storprocName = storproc.replaceAll("[0-9]", "");
            if (mapName.contains(".")) {
                storproc = mapName.substring(0, mapName.lastIndexOf("."));
                storprocName = storproc.replaceAll("[0-9]", "");
            }
            removeExtradots(mapSPecsLists);
            changeresultofTarget(mapSPecsLists);
            removebraces(mapSPecsLists);
            for (MappingSpecificationRow mapSPecs : mapSPecsLists) {
                String Sourcetablename = mapSPecs.getSourceTableName();
               
                if (Sourcetablename.split("\n").length == 1) {
                if (Sourcetablename.contains(".")) {
                    if (Sourcetablename.split("\\.").length > 2) {
                        Sourcetablename = Sourcetablename.split("\\.")[Sourcetablename.split("\\.").length - 2] + "." + Sourcetablename.split("\\.")[Sourcetablename.split("\\.").length - 1];
                    }
                }
                }
               
                sourceNamesSet(Sourcetablename.trim().toUpperCase(), envMap, mapSPecs, mapName, storprocName, folderName, sysEnvTableDetails, tableColDetails, smutill);
                String targetTableName = mapSPecs.getTargetTableName();
               
                if (targetTableName.split("\n").length == 1) {
                if (targetTableName.contains(".")) {
                    if (targetTableName.split("\\.").length > 2) {
                        targetTableName = targetTableName.split("\\.")[targetTableName.split("\\.").length - 2] + "." + targetTableName.split("\\.")[targetTableName.split("\\.").length - 1];
                    }
                }
                }
               
                targetNameSet(targetTableName.trim().toUpperCase(), envMap, mapSPecs, mapName, storprocName, folderName, sysEnvTableDetails, tableColDetails, smutill);
                addSourceColumnsDetails(mapSPecs, Sourcetablename, sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
                addTargetColumnsDetails(mapSPecs, targetTableName.trim().toUpperCase(), sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
            }

            String mapjson = mapper.writeValueAsString((Object) mapObj);
            return mapjson;
        } catch (Exception ex) {
             LOGGER.error("Exception Occured at Syncmetadata_Zovio_V2 inside getSourceSystem()>>", ex);
            return null;
        }
    }

    public static void sourceNamesSet(String Sourcetablename, List<String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName, String folderName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill) {

        if (Sourcetablename.split("\n").length > 1) {
            String[] sourcetablecolumn = Sourcetablename.split("\n");
            List<String> sourcesystem = getSourceSystem(sourcetablecolumn, mapName, envMap, mapSPecs, storprocName, folderName, sysEnvTableDetails, tableColDetails, smutill);
            List<String> sourceenv = getSourceEnv(sourcetablecolumn, mapName, envMap, mapSPecs, storprocName, folderName, sysEnvTableDetails, tableColDetails, smutill);
            List<String> sourceTabc = getappendSourceTables(sourcetablecolumn, mapName);
            String sourceSystem = StringUtils.join(sourcesystem, "\n");
            String sourceEnv = StringUtils.join(sourceenv, "\n");
            String Sourcetables = StringUtils.join(sourceTabc, "\n");
            mapSPecs.setSourceSystemEnvironmentName(sourceEnv);
            mapSPecs.setSourceSystemName(sourceSystem);
            mapSPecs.setSourceTableName(Sourcetables);
        } else {
            if (!sysEnvTableDetails.containsKey(Sourcetablename)) {
                updateTableDetails(envMap, Sourcetablename, smutill, sysEnvTableDetails, tableColDetails);
            }
            if (sysEnvTableDetails.containsKey(Sourcetablename)) {
                String sourceenvSys = sysEnvTableDetails.get(Sourcetablename);

                String SystemName = sourceenvSys.split("###")[0];
                String environmentName = sourceenvSys.split("###")[1];
                mapSPecs.setSourceSystemName(SystemName);
                mapSPecs.setSourceSystemEnvironmentName(environmentName);
                mapSPecs.setSourceTableName(Sourcetablename);
            } else if (Sourcetablename.toUpperCase().contains("RESULT_OF_") || Sourcetablename.toUpperCase().contains("INSERT-SELECT") || Sourcetablename.toUpperCase().contains("UPDATE-SELECT") || Sourcetablename.toUpperCase().contains("RS-")) {
                mapSPecs.setSourceTableName("");
                mapSPecs.setSourceTableName(Sourcetablename + "_" + mapName);
            } else {
                mapSPecs.setSourceTableName(Sourcetablename);
            }
        }

    }

    public static void targetNameSet(String Targettablename, List<String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName, String folderName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill) {

        if (Targettablename.split("\n").length > 1) {
            String[] tgttablecolumn = Targettablename.split("\n");
            List<String> targetsystem = getTargetSystem(tgttablecolumn, mapName, envMap, mapSPecs, storprocName, folderName, sysEnvTableDetails, tableColDetails, smutill);
            List<String> targeteenv = gettargetEnv(tgttablecolumn, mapName, envMap, mapSPecs, storprocName, folderName, sysEnvTableDetails, tableColDetails, smutill);
            List<String> tgtTabc = getappendSourceTables(tgttablecolumn, mapName);
            String sourceSystem = StringUtils.join(targetsystem, "\n");
            String sourceEnv = StringUtils.join(targeteenv, "\n");
            String tgttab = StringUtils.join(tgtTabc, "\n");
            mapSPecs.setTargetSystemName(sourceSystem);
            mapSPecs.setTargetSystemEnvironmentName(sourceEnv);
            mapSPecs.setTargetTableName(tgttab);
        } else {
            if (!sysEnvTableDetails.containsKey(Targettablename)) {
                updateTableDetails(envMap, Targettablename, smutill, sysEnvTableDetails, tableColDetails);
            }
            if (sysEnvTableDetails.containsKey(Targettablename)) {
                String targetenvSys = sysEnvTableDetails.get(Targettablename);
                String SystemName = targetenvSys.split("###")[0];
                mapSPecs.setTargetSystemName(SystemName);
                String environmentName = targetenvSys.split("###")[1];
                mapSPecs.setTargetSystemEnvironmentName(environmentName);
                mapSPecs.setTargetTableName(Targettablename);
            } else if (Targettablename.toUpperCase().contains("RESULT_OF_") || Targettablename.toUpperCase().contains("INSERT-SELECT") || Targettablename.toUpperCase().contains("UPDATE-SELECT")|| Targettablename.toUpperCase().contains("RS-")) {
                mapSPecs.setTargetTableName("");
                mapSPecs.setTargetTableName(Targettablename + "_" + mapName);
            } else {
                mapSPecs.setTargetTableName(Targettablename);
            }
        }

    }

    public static List<String> getSourceSystem(String[] sourcetablename, String mapname, List<String> envMap, MappingSpecificationRow mapSPecs, String storprocName, String folderName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill) {
        List<String> sourcesystem = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if (sourceTab.contains(".")) {
                    if (sourceTab.split("\\.").length > 2) {
                        sourceTab = sourceTab.split("\\.")[sourceTab.split("\\.").length - 2] + "." + sourceTab.split("\\.")[sourceTab.split("\\.").length - 1];
                    }
                }
                if (!sysEnvTableDetails.containsKey(sourceTab)) {
                    updateTableDetails(envMap, sourceTab, smutill, sysEnvTableDetails, tableColDetails);
                }
                if (sysEnvTableDetails.containsKey(sourceTab)) {
                    String sourceenvSys = sysEnvTableDetails.get(sourceTab);
                    sourcesystem.add(sourceenvSys.split("###")[0]);
                } else {
                    String SystemName = mapSPecs.getTargetSystemName();
                    sourcesystem.add(SystemName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Exception Occured at Syncmetadata_Zovio_V2 inside getSourceSystem()>>", e);
        }
        return sourcesystem;
    }

    public static List<String> getTargetSystem(String[] targettablename, String mapname, List<String> envMap, MappingSpecificationRow mapSPecs, String storprocName, String folderName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill) {
        List<String> tgtsystem = new LinkedList<>();
        try {
            for (String targetTab : targettablename) {

                if (targetTab.contains(".")) {
                    if (targetTab.split("\\.").length > 2) {
                        targetTab = targetTab.split("\\.")[targetTab.split("\\.").length - 2] + "." + targetTab.split("\\.")[targetTab.split("\\.").length - 1];
                    }
                }

                if (!sysEnvTableDetails.containsKey(targetTab)) {
                    updateTableDetails(envMap, targetTab, smutill, sysEnvTableDetails, tableColDetails);
                }
                if (sysEnvTableDetails.containsKey(targetTab)) {
                    String sourceenvSys = sysEnvTableDetails.get(targetTab);
                    tgtsystem.add(sourceenvSys.split("###")[0]);
                } else {
                    String SystemName = mapSPecs.getTargetSystemName();
                    tgtsystem.add(SystemName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Exception Occured at Syncmetadata_Zovio_V2 inside getTargetSystem()>>", e);
        }
        return tgtsystem;
    }

    public static List<String> getSourceEnv(String[] sourcetablename, String mapname, List<String> envMap, MappingSpecificationRow mapSPecs, String storprocName, String folderName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill) {
        List<String> sourceenv = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if (sourceTab.contains(".")) {
                    if (sourceTab.split("\\.").length == 3) {
                        sourceTab = sourceTab.split("\\.")[1] + "." + sourceTab.split("\\.")[2];
                    }
                }
                if (!sysEnvTableDetails.containsKey(sourceTab)) {
                    updateTableDetails(envMap, sourceTab, smutill, sysEnvTableDetails, tableColDetails);
                }
                if (sysEnvTableDetails.containsKey(sourceTab)) {
                    String sourceenvSys = sysEnvTableDetails.get(sourceTab);
                    sourceenv.add(sourceenvSys.split("###")[1]);
                } else {
                    String envName = mapSPecs.getTargetSystemEnvironmentName();
                    sourceenv.add(envName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Exception Occured at Syncmetadata_Zovio_V2 inside getSourceEnv()>>", e);
        }
        return sourceenv;
    }

    public static List<String> gettargetEnv(String[] targettablename, String mapname, List<String> envMap, MappingSpecificationRow mapSPecs, String storprocName, String folderName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill) {
        List<String> targetenv = new LinkedList<>();
        try {
            for (String targetTab : targettablename) {
                if (targetTab.contains(".")) {
                    if (targetTab.split("\\.").length == 3) {
                        targetTab = targetTab.split("\\.")[1] + "." + targetTab.split("\\.")[2];
                    }
                }
                if (!sysEnvTableDetails.containsKey(targetTab)) {
                    updateTableDetails(envMap, targetTab, smutill, sysEnvTableDetails, tableColDetails);
                }
                if (sysEnvTableDetails.containsKey(targetTab)) {
                    String sourceenvSys = sysEnvTableDetails.get(targetTab);
                    targetenv.add(sourceenvSys.split("###")[1]);
                } else {
                    String envName = mapSPecs.getTargetSystemEnvironmentName();
                    targetenv.add(envName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return targetenv;
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
                if (sourceTab.toUpperCase().contains("RESULT_OF_") || sourceTab.toUpperCase().contains("INSERT-SELECT") || sourceTab.toUpperCase().contains("UPDATE-SELECT") || sourceTab.toUpperCase().contains("RS-")) {
                    sourceTab = sourceTab + "_" + MapName;
                }
                sourcTables.add(sourceTab);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourcTables;
    }

    public static void changeresplitTable(ArrayList<MappingSpecificationRow> mapspeclist) {
        try {
            Set<String> targettabset = new LinkedHashSet();
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                String targetTabName = row.getTargetTableName();
                String SourceTableName = row.getSourceTableName();
                String[] Sourcetablenamearray = SourceTableName.split("\n");
                String Srctablename = "";
                String TgtTabName = "";
                for (String Sourcetablename : Sourcetablenamearray) {
                    String stab[] = Sourcetablename.split("\\.");
                    if (stab.length >= 2) {
                        Srctablename = stab[stab.length - 2] + "." + stab[stab.length - 1];
                    }
                }
                String[] targetTabNamearray = targetTabName.split("\n");
                for (String TargetTabName : targetTabNamearray) {
                    String ttab[] = TargetTabName.split("\\.");
                    if (ttab.length >= 2) {
                        TgtTabName = ttab[ttab.length - 2] + "." + ttab[ttab.length - 1];
                    }
                }
                if (!Srctablename.isEmpty()) {
                    row.setSourceTableName(Srctablename);
                }
                if (!TgtTabName.isEmpty()) {
                    row.setTargetTableName(TgtTabName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Exception Occured at Syncmetadata_Zovio_V2 inside changeresplitTable()>>", e);
        }
    }

    public static void changeresultofTarget(ArrayList<MappingSpecificationRow> mapspeclist) {
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
                String[] Sourcetablenamearray = SourceTableName.split("\n");
                for (String Sourcetablename : Sourcetablenamearray) {
                    if (!Sourcetablename.toUpperCase().contains("RESULT") && !Sourcetablename.toUpperCase().contains("INSERT") && !Sourcetablename.toUpperCase().contains("UPDATE") && !Sourcetablename.toUpperCase().startsWith("#") && !Sourcetablename.toUpperCase().contains("RS-")) {
                        if (!Sourcetablename.contains(".") && !"".equals(Sourcetablename)) {
                            Sourcetablename = "dbo." + Sourcetablename;
                            sourcetabsb.append(Sourcetablename).append("\n");
                        }
                    }
                }
                String[] targetTabNamearray = targetTabName.split("\n");
                for (String TargetTabName : targetTabNamearray) {
                    if (!TargetTabName.toUpperCase().contains("RESULT") && !TargetTabName.toUpperCase().contains("INSERT") && !TargetTabName.toUpperCase().contains("UPDATE") && !TargetTabName.toUpperCase().startsWith("#") && !TargetTabName.toUpperCase().contains("RS-")) {
                        if (!TargetTabName.contains(".") && !"".equals(TargetTabName)) {
                            TargetTabName = "dbo." + TargetTabName;
                            targettabsb.append(TargetTabName).append("\n");
                        }
                    }
                }
                if (!sourcetabsb.toString().isEmpty()) {
                    row.setSourceTableName(sourcetabsb.toString().trim());
                } else {
                    row.setSourceTableName(SourceTableName);
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
            e.printStackTrace();
            LOGGER.error("Exception Occured at Syncmetadata_Zovio_V2 inside changeresultofTarget()>>", e);
        }
    }

    public static void removebraces(ArrayList<MappingSpecificationRow> mappingspec) {

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
            e.printStackTrace();
        }

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
            LOGGER.error( "Exception Occured at Syncmetadata_Natixix_V2 inside addSourceColumnsDetails()>>", e);
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
            LOGGER.error( "Exception Occured at Syncmetadata_Natixix_V2 inside addSourceColumnsDetails()>>", e);
        }
        
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
            LOGGER.error( "Exception Occured at Syncmetadata_Natixix_V2 inside addTargetColumnsDetails()>>", e);
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
                            LOGGER.error("Exception Occured at Syncmetadata_Natixix_V2 inside updateColDetails()>>", e);
                        }
                        
                    }
                    return columnMap.get(colId);
                }
            }
        }
        return null;
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
            LOGGER.error("Exception Occured at Syncmetadata_Natixix_V2 inside addSourceColumnsDetails()>>", e);
        }
        
    }
    public static void updateTableDetails(List<String> sysEnvDetails, String tableName, SystemManagerUtil smUtil, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails) {
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
                LOGGER.error("Exception Occured at Syncmetadata_Zovio_V2 inside updateTableDetails()>>", e);
            }

        }
    }
    public static void removeExtradots(ArrayList<MappingSpecificationRow> mapspeclist)
    {
          Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                
                String targetTabName = row.getTargetTableName().replace("[", "").replace("]", "");
                String SourceTableName = row.getSourceTableName().replace("[", "").replace("]", "");
                if(SourceTableName.startsWith("."))
                {
                    SourceTableName=SourceTableName.replace(".","");
                }
                while(SourceTableName.contains("..")){
                    SourceTableName=SourceTableName.replace("..", ".");
                }
                row.setSourceTableName(SourceTableName);
                if(targetTabName.startsWith("."))
                {
                    targetTabName=targetTabName.replace(".","");
                }
                while(targetTabName.contains("..")){
                    targetTabName=targetTabName.replace("..", ".");
                }
                row.setTargetTableName(targetTabName);
            }       
        
    }
}
