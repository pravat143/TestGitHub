/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.metadata;

import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.sm.SMColumn;
import com.ads.api.util.SystemManagerUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 *
 * @author AmanSingh
 */
public class Syncmetadata_Natixis_V2_1 {

    private static final Logger LOGGER = Logger.getLogger(Syncmetadata_Natixis_V2_1.class.getName());

    public String metadataSync(List<String> envMap, SystemManagerUtil smutill, String json, String folderName, int projectid) {
        String jsonvalue = "";
        try {
            Map<String, String> sysEnvTableDetails = new HashMap<>();
            Map<String, Map<String, Integer>> tableColDetails = new HashMap<>();
            Map<String, Map<Integer, SMColumn>> ColsDetails = new HashMap<>();
            ObjectMapper mapper = new ObjectMapper();
            json = json.replace(",\"childNodes\":[]", "");
            List<Mapping> mapObj = (List<Mapping>) mapper.readValue(json, (TypeReference) new TypeReference<List<Mapping>>() {
            });
            ArrayList<MappingSpecificationRow> mapSPecsLists = (ArrayList<MappingSpecificationRow>) mapObj.get(0).getMappingSpecifications();
            folderName = folderName.split("\\.")[0];
            changeresultofTarget(mapSPecsLists);
            
            removebraces(mapSPecsLists);
            changeinsertselect(mapSPecsLists);
            dolookupwithstarcolumns(mapSPecsLists);
            String mapName = mapObj.get(0).getMappingName();
            String storproc = mapName;
            String storprocName = storproc.replaceAll("[0-9]", "");
            if (mapName.contains(".")) {
                storproc = mapName.substring(0, mapName.lastIndexOf("."));
                storprocName = storproc.replaceAll("[0-9]", "");
            }
            for (MappingSpecificationRow mapSPecs : mapSPecsLists) {
                String sourcesystemName = "";
                String sourceenvName = "";
                String Sourcetablename = mapSPecs.getSourceTableName();

                    if (Sourcetablename.contains(".")) {
           String  Sourcetablenames[]=Sourcetablename.split("\n");
                   for (String Sourcetablename1 : Sourcetablenames) {
                     if (Sourcetablename1.split("\\.").length >= 4) {
                        List<String> systemarr = new LinkedList();
                        String[] srctabarr = Sourcetablename1.split("\\.");
                        int i = 0;
                        for (String system : srctabarr) {
                            if (i != Sourcetablename1.split("\\.").length - 3) {
                                systemarr.add(system);

                            }
                            if (i == Sourcetablename1.split("\\.").length - 3) {
                                break;
                            }

                            i++;
                        }

                        sourcesystemName = StringUtils.join(systemarr, ".");
                        sourceenvName = Sourcetablename1.split("\\.")[Sourcetablename1.split("\\.").length - 3];
                    }
                  }
                    if(Sourcetablename.split("\n").length == 1)
                    {
                    if (Sourcetablename.split("\\.").length >= 3) {
                        Sourcetablename = Sourcetablename.split("\\.")[Sourcetablename.split("\\.").length - 2] + "." + Sourcetablename.split("\\.")[Sourcetablename.split("\\.").length - 1];
                    }
                    }

                }
                sourceNamesSet(Sourcetablename.trim().toUpperCase(), envMap, mapSPecs, mapName, storprocName, folderName, sourceenvName, sourcesystemName, sysEnvTableDetails, tableColDetails, smutill);
                String targetTableName = mapSPecs.getTargetTableName();
                if (!targetTableName.toUpperCase().contains("RESULT") && !targetTableName.toUpperCase().contains("INSERT") && !targetTableName.toUpperCase().contains("UPDATE")) {
                    if (!targetTableName.contains(".") && !"".equals(targetTableName) && !targetTableName.startsWith("#")) {

                        targetTableName = "dbo." + targetTableName;

                    }
                }
                targetNameSet(targetTableName.trim().toUpperCase(), envMap, mapSPecs, mapName, storprocName, folderName, sysEnvTableDetails, tableColDetails, smutill);
                addSourceColumnsDetails(mapSPecs, Sourcetablename, sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
                addTargetColumnsDetails(mapSPecs, targetTableName.trim().toUpperCase(), sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
            }
            changeresplitTable(mapSPecsLists);
            String mapjson = mapper.writeValueAsString((Object) mapObj);
            return mapjson;

        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Exception Occured in Syncmetadata_Natixis_V2 class metadataSync() method >> ", ex);
            return null;
        }
    }

    public void sourceNamesSet(String Sourcetablename, List<String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName, String folderName, String sourceenvName, String sourceSystemName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill) {
        if (mapSPecs.getSourceTableName().split("\\.").length < 1) {
        if (mapSPecs.getSourceTableName().split("\\.").length >= 4) {
            if (!"".equals(sourceenvName)) {
                mapSPecs.setSourceSystemEnvironmentName(sourceenvName);

            }
            if (!"".equals(sourceSystemName)) {
                mapSPecs.setSourceSystemName(sourceSystemName);

            }
         }
        } else {
            StringBuilder tables=new StringBuilder();
            if (Sourcetablename.split("\n").length > 1) {
                
                String[] sourcetablecolumn = Sourcetablename.split("\n");
                for (String sourcetables : sourcetablecolumn) {
                    if(sourcetables.split("\\.").length>=3)
                    {
                 sourcetables=sourcetables.split("\\.")[sourcetables.split("\\.").length-2]+"."+sourcetables.split("\\.")[sourcetables.split("\\.").length-1];
                    }
                 tables.append(sourcetables).append("\n");
                }
               String srctabcolumns[]=tables.toString().split("\n");
                List<String> sourcesystem = getSourceSystem(srctabcolumns, mapName, envMap, mapSPecs, storprocName, folderName, sysEnvTableDetails, tableColDetails, smutill);
                List<String> sourceenv = getSourceEnv(srctabcolumns, mapName, envMap, mapSPecs, storprocName, folderName, sysEnvTableDetails, tableColDetails, smutill);
                List<String> sourceTabc = getappendSourceTables(sourcetablecolumn,mapName);
                String Sourcetables = StringUtils.join(sourceTabc, "\n");
                String sourceSystem = StringUtils.join(sourcesystem, "\n");
                String sourceEnv = StringUtils.join(sourceenv, "\n");
                mapSPecs.setSourceTableName(Sourcetables);
                mapSPecs.setSourceSystemEnvironmentName(sourceEnv);
                mapSPecs.setSourceSystemName(sourceSystem);
            } else {
                if (!sysEnvTableDetails.containsKey(Sourcetablename)) {
                    updateTableDetails(envMap, Sourcetablename, smutill, sysEnvTableDetails, tableColDetails);
                }
                if (sysEnvTableDetails.containsKey(Sourcetablename)) {
                    String sourceenvSys = sysEnvTableDetails.get(Sourcetablename);
                    String SystemName = sourceenvSys.split("###")[0];
                    mapSPecs.setSourceSystemName(SystemName);
                    String environmentName = sourceenvSys.split("###")[1];
                    mapSPecs.setSourceSystemEnvironmentName(environmentName);

                }
            }
        }
        mapSPecs.setSourceTableName(Sourcetablename);
        if (Sourcetablename.split("\n").length > 1) {
            String[] srctabNamearr = Sourcetablename.split("\n");
            StringBuilder sb1 = new StringBuilder();
            for (String srctabName : srctabNamearr) {
                if (srctabName.toUpperCase().contains("RESULT_OF_") || srctabName.toUpperCase().contains("INSERT-SELECT") || srctabName.toUpperCase().contains("UPDATE-")) {
                    sb1.append(srctabName + "_" + mapName).append("\n");
                } else {

                    sb1.append(srctabName).append("\n");
                }
            }
            mapSPecs.setSourceTableName(sb1.toString());
        } else {
            if (Sourcetablename.toUpperCase().contains("RESULT_OF_") || Sourcetablename.toUpperCase().contains("INSERT-SELECT") || Sourcetablename.toUpperCase().contains("UPDATE-")) {
                mapSPecs.setSourceTableName("");
                mapSPecs.setSourceTableName(Sourcetablename + "_" + mapName);
            }
        }

    }

    public void targetNameSet(String Targettablename, List<String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName, String folderName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill) {
        if (Targettablename.split("\n").length > 1) {
            String[] tgttablecolumn = Targettablename.split("\n");
            List<String> sourcesystem = getSourceSystem(tgttablecolumn, mapName, envMap, mapSPecs, storprocName, folderName, sysEnvTableDetails, tableColDetails, smutill);
            List<String> sourceenv = getSourceEnv(tgttablecolumn, mapName, envMap, mapSPecs, storprocName, folderName, sysEnvTableDetails, tableColDetails, smutill);
            String sourceSystem = StringUtils.join(sourcesystem, "\n");
            String sourceEnv = StringUtils.join(sourceenv, "\n");

            mapSPecs.setTargetSystemName(sourceSystem);
            mapSPecs.setTargetSystemEnvironmentName(sourceEnv);
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

            }
            //if (envMap.containsKey(Targettablename))

        }
        mapSPecs.setTargetTableName(Targettablename);
        if (Targettablename.split("\n").length > 1) {
            StringBuilder sb1 = new StringBuilder();

            String[] srctabNamearr = Targettablename.split("\n");

            for (String trgetatbName : srctabNamearr) {
                if (trgetatbName.toUpperCase().contains("RESULT_OF_") || trgetatbName.toUpperCase().contains("INSERT-SELECT") || trgetatbName.toUpperCase().contains("UPDATE-")) {
                    sb1.append(trgetatbName + "_" + mapName).append("\n");
                } else {
                    sb1.append(trgetatbName).append("\n");

                }
            }
            mapSPecs.setTargetTableName(sb1.toString());
        } else {
            if (Targettablename.toUpperCase().contains("RESULT_OF_") || Targettablename.toUpperCase().contains("INSERT-SELECT") || Targettablename.toUpperCase().contains("UPDATE-")) {

                mapSPecs.setTargetTableName(Targettablename + "_" + mapName);
            }
        }
    }

    public List<String> getSourceSystem(String[] sourcetablename, String mapname, List<String> envMap, MappingSpecificationRow mapSPecs, String storprocName, String folderName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill) {
        List<String> sourcesystem = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
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
            Logger.getLogger(Syncmetadata_Natixis.class.getName()).log(Level.SEVERE, null, e);
        }
        return sourcesystem;
    }

    public List<String> getSourceEnv(String[] sourcetablename, String mapname, List<String> envMap, MappingSpecificationRow mapSPecs, String storprocName, String folderName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, SystemManagerUtil smutill) {
        List<String> sourceenv = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
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
        }
        return sourceenv;
    }

    public void changeresplitTable(ArrayList<MappingSpecificationRow> mapspeclist) {
        try {
            Set<String> targettabset = new LinkedHashSet();
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                StringBuilder sourcetabsb = new StringBuilder();
                StringBuilder targettabsb = new StringBuilder();
                String targetTabName = row.getTargetTableName();
                String SourceTableName = row.getSourceTableName();
                String[] Sourcetablenamearray = SourceTableName.split("\n");
                String Srctablename = "";
                String TgtTabName = "";
                for (String Sourcetablename : Sourcetablenamearray) {
                    String stab[] = Sourcetablename.split("\\.");
                    if (stab.length >= 2) {
                        Srctablename = stab[stab.length - 2] + "." + stab[stab.length - 1];
                        sourcetabsb.append(Srctablename).append("\n");
                    }
                }
                String[] targetTabNamearray = targetTabName.split("\n");
                for (String TargetTabName : targetTabNamearray) {
                    String ttab[] = TargetTabName.split("\\.");
                    if (ttab.length >= 2) {
                        TgtTabName = ttab[ttab.length - 2] + "." + ttab[ttab.length - 1];
                        targettabsb.append(TgtTabName).append("\n");
                    }
                }
                if (!sourcetabsb.toString().isEmpty()) {
                    row.setSourceTableName(sourcetabsb.toString());
                }
                if (!targettabsb.toString().isEmpty()) {
                    row.setTargetTableName(targettabsb.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Logger.getLogger(Syncmetadata_Natixis.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    public void changeresultofTarget(ArrayList<MappingSpecificationRow> mapspeclist) {
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
                    if (!Sourcetablename.toUpperCase().contains("RESULT") && !Sourcetablename.toUpperCase().contains("INSERT") && !Sourcetablename.toUpperCase().contains("UPDATE") && !Sourcetablename.toUpperCase().startsWith("#")) {
                        if (!Sourcetablename.contains(".") && !"".equals(Sourcetablename)) {
                            Sourcetablename = "dbo." + Sourcetablename;
                            sourcetabsb.append(Sourcetablename).append("\n");
                        }
                    }
                }
                String[] targetTabNamearray = targetTabName.split("\n");
                for (String TargetTabName : targetTabNamearray) {
                    if (!TargetTabName.toUpperCase().contains("RESULT") && !TargetTabName.toUpperCase().contains("INSERT") && !TargetTabName.toUpperCase().contains("UPDATE") && !TargetTabName.toUpperCase().startsWith("#")) {
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
            Logger.getLogger(Syncmetadata_Natixis.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    public static Mapping createMapFromMappingSpecifiactionRow(ArrayList<MappingSpecificationRow> specrowlist, String mapfileName, String query, int projectid) {
        Mapping mapping = new Mapping();
        mapping.setMappingName(mapfileName);
        mapping.setMappingSpecifications(specrowlist);
        mapping.setSourceExtractQuery(query);
        mapping.setProjectId(projectid);
        // mapping.setSubjectId(subjectid);
        return mapping;
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
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static List<String> getColumnsfromTableName(ArrayList<MappingSpecificationRow> mapspeclist, String tableName) {
        List<String> columns = new LinkedList<>();
        try {

            for (MappingSpecificationRow mappingSpecificationRow : mapspeclist) {

                String sourceTableName = mappingSpecificationRow.getSourceTableName();

                if (tableName.equalsIgnoreCase(sourceTableName)) {
                    columns.add(mappingSpecificationRow.getSourceColumnName());

                }

            }

        } catch (Exception e) {

            e.printStackTrace();
        }
        return columns;
    }

    public static void dolookupwithstarcolumns(ArrayList<MappingSpecificationRow> mapspeclist) {
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
            LOGGER.log(Level.SEVERE, "Exception Occured at Syncmetadata_Natixix_V2 inside updateColDetails()>>", e);
        }

    }

    private void addSourceColumnsDetails(MappingSpecificationRow mapSpec, String columnName, String tableName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, Map<String, Map<Integer, SMColumn>> ColsDetails, List<String> sysEnvDetails, SystemManagerUtil smUtil) {

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
            LOGGER.log(Level.SEVERE, "Exception Occured at Syncmetadata_Natixix_V2 inside addSourceColumnsDetails()>>", e);
        }

    }

    private void addTargetColumnsDetails(MappingSpecificationRow mapSpec, String columnName, String tableName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, Map<String, Map<Integer, SMColumn>> ColsDetails, List<String> sysEnvDetails, SystemManagerUtil smUtil) {

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
            LOGGER.log(Level.SEVERE, "Exception Occured at Syncmetadata_Natixix_V2 inside addTargetColumnsDetails()>>", e);
        }

    }

    private SMColumn getColDetails(String columnName, String tableName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, Map<String, Map<Integer, SMColumn>> ColsDetails, List<String> sysEnvDetails, SystemManagerUtil smUtil) {
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
                            LOGGER.log(Level.SEVERE, "Exception Occured at Syncmetadata_Natixix_V2 inside updateColDetails()>>", e);
                        }

                    }
                    return columnMap.get(colId);
                }
            }
        }
        return null;
    }

    private void updateTableDetails(List<String> sysEnvDetails, String tableName, SystemManagerUtil smUtil, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails) {
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
                LOGGER.log(Level.SEVERE, "Exception Occured at Syncmetadata_Natixix_V2 inside updateTableDetails()>>", e);
            }

        }
    }

    private void addTargetColumnsDetails(MappingSpecificationRow mapSPecs, String targettablename, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, Map<String, Map<Integer, SMColumn>> ColsDetails, List<String> envMap, SystemManagerUtil smutill) {
        try {
            if (targettablename.split("\n").length > 1) {
                String targetTable = targettablename.split("\n")[0];
                String targetColumn = mapSPecs.getTargetColumnName().split("\n")[0];
                addTargetColumnsDetails(mapSPecs, targetTable, targetColumn, sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
            } else {
                addTargetColumnsDetails(mapSPecs, targettablename, mapSPecs.getTargetColumnName(), sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception Occured at Syncmetadata_Natixix_V2 inside addSourceColumnsDetails()>>", e);
        }

    }

    private void addSourceColumnsDetails(MappingSpecificationRow mapSPecs, String Sourcetablename, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, Map<String, Map<Integer, SMColumn>> ColsDetails, List<String> envMap, SystemManagerUtil smutill) {
        try {
            if (Sourcetablename.split("\n").length > 1) {
                String sourceTable = Sourcetablename.split("\n")[0];
                String sourceColumn = mapSPecs.getSourceColumnName().split("\n")[0];
                addSourceColumnsDetails(mapSPecs, sourceTable, sourceColumn, sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
            } else {
                addSourceColumnsDetails(mapSPecs, Sourcetablename, mapSPecs.getSourceColumnName(), sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception Occured at Syncmetadata_Natixix_V2 inside addSourceColumnsDetails()>>", e);
        }

    }

    public static void changeinsertselect(ArrayList<MappingSpecificationRow> mapspeclist) {
        try {
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                String sourceTableName = row.getSourceTableName();
                String columnName = row.getSourceColumnName();

                if (sourceTableName.toUpperCase().contains("INSERT")) {
                 if(!checkinserttarget(mapspeclist,sourceTableName,columnName)){
                   iter.remove();
                 }
                }

            }

        } catch (Exception e) {

            e.printStackTrace();
        }

    }

    public static boolean checkinserttarget(ArrayList<MappingSpecificationRow> mapspeclist, String tableName, String ColumnName) {

        try {
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                String targetTableName = row.getTargetTableName();
                String targetColumnName = row.getTargetColumnName();
                
                if(targetTableName.equalsIgnoreCase(tableName)&&targetColumnName.equalsIgnoreCase(ColumnName)){
                
                return true;
                }
            }

        } catch (Exception e) {

            e.printStackTrace();
        }
         return false;
    }
    public static List<String> getappendSourceTables(String[] sourcetablename,String MapName) {
        List<String> sourcTables = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if (sourceTab.contains(".")) {
                    if (sourceTab.split("\\.").length > 2) {
                        sourceTab = sourceTab.split("\\.")[sourceTab.split("\\.").length-2] + "." + sourceTab.split("\\.")[sourceTab.split("\\.").length-1];
                    }
                }
       if(sourceTab.toUpperCase().contains("RESULT_OF_") || sourceTab.toUpperCase().contains("INSERT-SELECT") || sourceTab.toUpperCase().contains("UPDATE-SELECT"))
            {
             sourceTab=sourceTab+MapName;
            }
               sourcTables.add(sourceTab);
        }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return sourcTables;
    }
} 
 