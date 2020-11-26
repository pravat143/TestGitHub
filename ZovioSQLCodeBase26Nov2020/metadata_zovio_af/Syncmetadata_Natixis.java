/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.metadata_zovio_af;

import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 *
 * @author InkolluReddy
 */
public class Syncmetadata_Natixis {

    public static String metadataSync(Map<String, String> envMap, SystemManagerUtil smutill, String json, String folderName, int projectid) {
        String jsonvalue = "";
        try {
            ObjectMapper mapper = new ObjectMapper();
            json = json.replace(",\"childNodes\":[]", "");
            List<Mapping> mapObj = (List<Mapping>) mapper.readValue(json, (TypeReference) new TypeReference<List<Mapping>>() {
            });
            //    ArrayList<MappingSpecificationRow> mapSPecsLists = mapping.getMappingSpecifications();          
            ArrayList<MappingSpecificationRow> mapSPecsLists = (ArrayList<MappingSpecificationRow>) mapObj.get(0).getMappingSpecifications();
            envMap.toString();
            folderName = folderName.split("\\.")[0];
            changeresultofTarget(mapSPecsLists);

            removebraces(mapSPecsLists);
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
                if (Sourcetablename.split("\n").length > 1) {
                    Sourcetablename = Sourcetablename.split("\n")[0];
                }
                if (Sourcetablename.contains(".")) {

                    if (Sourcetablename.split("\\.").length >= 4) {
                        List<String> systemarr = new LinkedList();
                        String[] srctabarr = Sourcetablename.split("\\.");
                        int i = 0;
                        for (String system : srctabarr) {
                            if (i != Sourcetablename.split("\\.").length - 3) {
                                systemarr.add(system);

                            }
                            if (i == Sourcetablename.split("\\.").length - 3) {
                                break;
                            }

                            i++;
                        }

                        sourcesystemName = StringUtils.join(systemarr, ".");
                        sourceenvName = Sourcetablename.split("\\.")[Sourcetablename.split("\\.").length - 3];
                    }

                    if (Sourcetablename.split("\\.").length >= 3) {
                        Sourcetablename = Sourcetablename.split("\\.")[Sourcetablename.split("\\.").length - 2] + "." + Sourcetablename.split("\\.")[Sourcetablename.split("\\.").length - 1];

                    }

                }
                sourceNamesSet(Sourcetablename.trim().toUpperCase(), envMap, mapSPecs, mapName, storprocName, folderName, sourceenvName, sourcesystemName);
                String targetTableName = mapSPecs.getTargetTableName();
                if (!targetTableName.toUpperCase().contains("RESULT") && !targetTableName.toUpperCase().contains("INSERT") && !targetTableName.toUpperCase().contains("UPDATE")) {
                    if (!targetTableName.contains(".") && !"".equals(targetTableName)) {
                        targetTableName = "dbo." + targetTableName;

                    }
                }
                targetNameSet(targetTableName.trim().toUpperCase(), envMap, mapSPecs, mapName, storprocName, folderName);
            }
            //  changeresplitTable(mapSPecsLists);
            changeresplitTable(mapSPecsLists);
         //   dolookupwithstarcolumns(mapSPecsLists);
            String mapjson = mapper.writeValueAsString((Object) mapObj);
            return mapjson;
            // String query = mapping.getSourceExtractQuery();
            // Mapping mapingobj =createMapFromMappingSpecifiactionRow(mapSPecsLists, mapName, query, projectid);

            //   return mapingobj;
        } catch (Exception ex) {
            Logger.getLogger(Syncmetadata_Natixis.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    public static void sourceNamesSet(String Sourcetablename, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName, String folderName, String sourceenvName, String sourceSystemName) {
        if (Sourcetablename.split("\n").length > 1) {
            String[] sourcetablecolumn = Sourcetablename.split("\n");
            envMap.toString();
            List<String> sourcesystem = getSourceSystem(sourcetablecolumn, mapName, envMap, mapSPecs, storprocName, folderName);
            List<String> sourceenv = getSourceEnv(sourcetablecolumn, mapName, envMap, mapSPecs, storprocName, folderName);
            String sourceSystem = StringUtils.join(sourcesystem, "\n");
            String sourceEnv = StringUtils.join(sourceenv, "\n");

            mapSPecs.setSourceSystemEnvironmentName(sourceEnv);
            mapSPecs.setSourceSystemName(sourceSystem);
        } else if (envMap.containsKey(Sourcetablename)) {
            String sourceenvSys = envMap.get(Sourcetablename);

            String SystemName = sourceenvSys.split("#")[1];
            String environmentName = sourceenvSys.split("#")[0];
            mapSPecs.setSourceSystemName(SystemName);
            mapSPecs.setSourceSystemEnvironmentName(environmentName);
            mapSPecs.setSourceTableName(Sourcetablename);
        }
        if (Sourcetablename.split("\n").length > 1) {
            String[] srctabNamearr = Sourcetablename.split("\n");
            StringBuilder sb1 = new StringBuilder();
            for (String srctabName : srctabNamearr) {
                if (srctabName.toUpperCase().contains("RESULT_OF_") || srctabName.toUpperCase().contains("INSERT-SELECT") || srctabName.toUpperCase().contains("UPDATE-")) {
                    sb1.append(srctabName + "_" + mapName).append("\n");
                }else{
                
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

        if (!"".equals(sourceenvName)) {
            mapSPecs.setSourceSystemEnvironmentName(sourceenvName);

        }
        if (!"".equals(sourceSystemName)) {
            mapSPecs.setSourceSystemName(sourceSystemName);

        }
    }

    public static void targetNameSet(String Targettablename, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName, String folderName) {
        if (Targettablename.split("\n").length > 1) {
            String[] tgttablecolumn = Targettablename.split("\n");
            envMap.toString();
            List<String> sourcesystem = getSourceSystem(tgttablecolumn, mapName, envMap, mapSPecs, storprocName, folderName);
            List<String> sourceenv = getSourceEnv(tgttablecolumn, mapName, envMap, mapSPecs, storprocName, folderName);
            String sourceSystem = StringUtils.join(sourcesystem, "\n");
            String sourceEnv = StringUtils.join(sourceenv, "\n");

            mapSPecs.setTargetSystemName(sourceSystem);
            mapSPecs.setTargetSystemEnvironmentName(sourceEnv);
        } else if (envMap.containsKey(Targettablename)) {
            String targetenvSys = envMap.get(Targettablename);
            String SystemName = targetenvSys.split("#")[1];
            mapSPecs.setTargetSystemName(SystemName);
            String environmentName = targetenvSys.split("#")[0];
            mapSPecs.setTargetSystemEnvironmentName(environmentName);
        }
        if (Targettablename.split("\n").length > 1) { 
            StringBuilder sb1 = new StringBuilder();
            
            String[] srctabNamearr = Targettablename.split("\n");
           
            for (String trgetatbName : srctabNamearr) {
                if (trgetatbName.toUpperCase().contains("RESULT_OF_") || trgetatbName.toUpperCase().contains("INSERT-SELECT") || trgetatbName.toUpperCase().contains("UPDATE-")) {
                    sb1.append(trgetatbName + "_" + mapName).append("\n");
                }
                else{
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

    public static List<String> getSourceSystem(String[] sourcetablename, String mapname, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String storprocName, String folderName) {
        List<String> sourcesystem = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);

                    sourcesystem.add(sourceenvSys.split("#")[1]);
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

    public static List<String> getSourceEnv(String[] sourcetablename, String mapname, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String storprocName, String folderName) {
        List<String> sourceenv = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);

                    sourceenv.add(sourceenvSys.split("#")[0]);
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

    public static void changeresplitTable(ArrayList<MappingSpecificationRow> mapspeclist) {
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
            e.printStackTrace();
        }

    }
}
