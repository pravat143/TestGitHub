/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.metadata_zovio_af;

/**
 *
 * @author PrajnaSurya
 */
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import com.ads.api.beans.common.Document;
import com.ads.api.beans.common.Node;
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.sm.SMColumn;
import com.ads.api.util.KeyValueUtil;
import com.ads.api.util.MappingManagerUtil;
import com.ads.api.util.SystemManagerUtil;
import com.icc.util.RequestStatus;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import org.apache.commons.io.FileUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 *
 * @author Pravat
 */
public class Syncmetadata_AF_V2_Oracle {

    private static final Logger LOGGER = Logger.getLogger(Syncmetadata_AF_V2_Oracle.class);
   static String extreamTargetTableName="";
   public static HashMap<String,String> colDataType=new HashMap<>();
    public static List<Mapping> metadataSync(List<String> envMap,SystemManagerUtil smutill, String json, String subjectName,String sqlfilepath,String jsonMetadatapath,String folder) {
        String jsonvalue = "";
        envMap.clear();
        envMap=getAllEnviroment(jsonMetadatapath,folder);
        try {
            Map<String, String> sysEnvTableDetails = new HashMap<>();
            Map<String, Map<String, Integer>> tableColDetails = new HashMap<>();
            Map<String, Map<Integer, SMColumn>> ColsDetails = new HashMap<>();
            ObjectMapper mapper = new ObjectMapper();
            json = json.replace(",\"childNodes\":[]", "");
            String fromfileschemaName=sqlfilepath.split("\\\\")[sqlfilepath.split("\\\\").length-2];
            String fromfiledatabaseName=sqlfilepath.split("\\\\")[sqlfilepath.split("\\\\").length-3];
            List<Mapping> mapObj = (List) mapper.readValue(json, new TypeReference<List<Mapping>>() {
            });
            ArrayList<MappingSpecificationRow> mapSPecsLists = mapObj.get(0).getMappingSpecifications();
            envMap.toString();
            // envMap.toString();
            String mapName = mapObj.get(0).getMappingName();
            
            mapName = mapName.replaceAll("[^\\w\\s]+", "");
          
            String storproc = mapName;
            String storprocName = storproc.replaceAll("[0-9]", "");
            if (mapName.contains(".")) {
                storproc = mapName.substring(0, mapName.lastIndexOf("."));
                storprocName = storproc.replaceAll("[0-9]", "");
            }
            subjectName = subjectName.toUpperCase();
           // removeExtradots(mapSPecsLists,fromfileschemaName,envMap);
            changeresultofTarget(mapSPecsLists, subjectName.toUpperCase(),fromfileschemaName,mapName);
            removebraces(mapSPecsLists);
            List<MappingSpecificationRow> colstars = updatespecrowforstar(mapSPecsLists, envMap, sysEnvTableDetails, tableColDetails, smutill);
            mapSPecsLists.addAll(colstars);
            List<MappingSpecificationRow> insertcols = settingstarColsofInsert(mapSPecsLists);
            mapSPecsLists.addAll(insertcols);
            removestarcolumspec(mapSPecsLists);
            for (MappingSpecificationRow mapSPecs : mapSPecsLists) {
                boolean sourcenamesset=true;
                boolean targetnamesset=true;
                String Sourcetablename = mapSPecs.getSourceTableName();
                if (Sourcetablename.split("\n").length == 1) {
                    if (Sourcetablename.contains(".") && !Sourcetablename.contains("..")) {
                        if (Sourcetablename.split("\\.").length > 2) {
                            Sourcetablename = Sourcetablename.split("\\.")[Sourcetablename.split("\\.").length - 2] + "." + Sourcetablename.split("\\.")[Sourcetablename.split("\\.").length - 1];
                        }
                    }
                    if (Sourcetablename.contains("..")) {
                        String databaseName = Sourcetablename.split("\\..")[0].toUpperCase();
                        String withoutschmetablename = Sourcetablename.split("\\..")[1];
                        // envMap
                        for (int i = 0; i < envMap.size(); i++) {
                            String metadatadbName = envMap.get(i).split("###")[3].toUpperCase();
                            if (databaseName.equalsIgnoreCase(metadatadbName)) {
                                String Schemas = envMap.get(i).split("###")[4];
                                String schemas[] = Schemas.split(",");
                                for (String schema1 : schemas) {
                                    String srctab=schema1+"."+withoutschmetablename;
                                    updateTableDetails(envMap, srctab , smutill, sysEnvTableDetails, tableColDetails);
                                    if (sysEnvTableDetails.containsKey(srctab)) {
                                        String sourceenvSys = sysEnvTableDetails.get(srctab);
                                        String SystemName = sourceenvSys.split("###")[0];
                                        String environmentName = sourceenvSys.split("###")[1];
                                        mapSPecs.setSourceSystemName(SystemName);
                                        mapSPecs.setSourceSystemEnvironmentName(environmentName);
                                        mapSPecs.setSourceTableName(srctab);
                                       
                                        sourcenamesset=false;
                                        addSourceColumnsDetails(mapSPecs, srctab.trim().toUpperCase(), sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
                                       }
                                    }
                                 }
                              }
                           }
                        }
                     if(sourcenamesset)
                     {
                        if(Sourcetablename.trim().contains(".."))
                        {
                            Sourcetablename=Sourcetablename.replace("..","."+fromfileschemaName+".");
                            Sourcetablename=Sourcetablename.split("\\.")[Sourcetablename.split("\\.").length - 2] + "." + Sourcetablename.split("\\.")[Sourcetablename.split("\\.").length - 1];
                        }
                        sourceNamesSet(Sourcetablename.trim().toUpperCase(), envMap, mapSPecs, mapName, storprocName, subjectName, sysEnvTableDetails, tableColDetails, smutill);
                        addSourceColumnsDetails(mapSPecs, Sourcetablename.trim().toUpperCase(), sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
                     } 
                    String targetTableName = mapSPecs.getTargetTableName();

                    if (targetTableName.split("\n").length == 1) {
                        if (targetTableName.contains(".") && !targetTableName.contains("..")) {
                            if (targetTableName.split("\\.").length > 2) {
                                targetTableName = targetTableName.split("\\.")[targetTableName.split("\\.").length - 2] + "." + targetTableName.split("\\.")[targetTableName.split("\\.").length - 1];
                            }
                        }
                        if (targetTableName.contains("..")) {
                        String databaseName = targetTableName.split("\\..")[0].toUpperCase();
                        String withoutschmetablename = targetTableName.split("\\..")[1];
                        // envMap
                        for (int i = 0; i < envMap.size(); i++) {
                            String metadatadbName = envMap.get(i).split("###")[3].toUpperCase();
                            if (databaseName.equalsIgnoreCase(metadatadbName)) {
                                String Schemas = envMap.get(i).split("###")[4];
                                String schemas[] = Schemas.split(",");
                                for (String schema1 : schemas) {
                                    String tgttab=schema1+"."+withoutschmetablename;
                                    updateTableDetails(envMap, tgttab, smutill, sysEnvTableDetails, tableColDetails);
                                    if (sysEnvTableDetails.containsKey(tgttab)) {
                                        String sourceenvSys = sysEnvTableDetails.get(tgttab);
                                        String SystemName = sourceenvSys.split("###")[0];
                                        String environmentName = sourceenvSys.split("###")[1];
                                        mapSPecs.setTargetSystemName(SystemName);
                                        mapSPecs.setTargetSystemEnvironmentName(environmentName);
                                        mapSPecs.setTargetTableName(tgttab);
                                        addTargetColumnsDetails(mapSPecs,tgttab.trim().toUpperCase(), sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
                                        targetnamesset=false;
                                       }
                                    }
                              }
                         }
                      }
                    }
                    if(targetnamesset)
                    {
                      if(targetTableName.trim().contains(".."))
                        {
                            targetTableName=targetTableName.replace("..","."+fromfileschemaName+".");
                            targetTableName=targetTableName.split("\\.")[targetTableName.split("\\.").length - 2] + "." + targetTableName.split("\\.")[targetTableName.split("\\.").length - 1];
                        }
                    targetNameSet(targetTableName.trim().toUpperCase(), envMap, mapSPecs, mapName, storprocName, subjectName, sysEnvTableDetails, tableColDetails, smutill);
                    addTargetColumnsDetails(mapSPecs, targetTableName.trim().toUpperCase(), sysEnvTableDetails, tableColDetails, ColsDetails, envMap, smutill);
                    }
                }
              
            
            //  String mapjson = mapper.writeValueAsString((Object) mapObj);
            metadataDesign(mapObj);
            //modificationOfExtreamtarget(mapObj,mapName);
            removeDupilcateMapSpecRow(mapObj);
            return mapObj;
        } catch (Exception ex) {
            ex.printStackTrace();
            LOGGER.error("Exception Occured at Syncmetadata_Zovio_V2 inside getSourceSystem()>>", ex);
            return null;
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
                if (!Sourcetablename.toUpperCase().contains("RESULT") && !Sourcetablename.toUpperCase().contains("INSERT") && !Sourcetablename.toUpperCase().contains("UPDATE") && !Sourcetablename.toUpperCase().startsWith("#") && !Sourcetablename.toUpperCase().contains("RS-") && !Sourcetablename.toUpperCase().equalsIgnoreCase(folderName)) {
                    updateTableDetails(envMap, Sourcetablename, smutill, sysEnvTableDetails, tableColDetails);
                }
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
                if (!Targettablename.toUpperCase().contains("RESULT") && !Targettablename.toUpperCase().contains("INSERT") && !Targettablename.toUpperCase().contains("UPDATE") && !Targettablename.toUpperCase().startsWith("#") && !Targettablename.toUpperCase().contains("RS-") && !Targettablename.toUpperCase().equalsIgnoreCase(folderName)) {
                    updateTableDetails(envMap, Targettablename, smutill, sysEnvTableDetails, tableColDetails);
                }
            }
            if (sysEnvTableDetails.containsKey(Targettablename)) {
                String targetenvSys = sysEnvTableDetails.get(Targettablename);
                String SystemName = targetenvSys.split("###")[0];
                mapSPecs.setTargetSystemName(SystemName);
                String environmentName = targetenvSys.split("###")[1];
                mapSPecs.setTargetSystemEnvironmentName(environmentName);
                mapSPecs.setTargetTableName(Targettablename);
            } else if (Targettablename.toUpperCase().contains("RESULT_OF_") || Targettablename.toUpperCase().contains("INSERT-SELECT") || Targettablename.toUpperCase().contains("UPDATE-SELECT") || Targettablename.toUpperCase().contains("RS-")) {
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
                    if (!sourceTab.toUpperCase().contains("RESULT") && !sourceTab.toUpperCase().contains("INSERT") && !sourceTab.toUpperCase().contains("UPDATE") && !sourceTab.toUpperCase().startsWith("#") && !sourceTab.toUpperCase().contains("RS-")) {
                        updateTableDetails(envMap, sourceTab, smutill, sysEnvTableDetails, tableColDetails);
                    }
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
                    if (!targetTab.toUpperCase().contains("RESULT") && !targetTab.toUpperCase().contains("INSERT") && !targetTab.toUpperCase().contains("UPDATE") && !targetTab.toUpperCase().startsWith("#") && !targetTab.toUpperCase().contains("RS-")) {
                        updateTableDetails(envMap, targetTab, smutill, sysEnvTableDetails, tableColDetails);
                    }
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
                    if (!sourceTab.toUpperCase().contains("RESULT") && !sourceTab.toUpperCase().contains("INSERT") && !sourceTab.toUpperCase().contains("UPDATE") && !sourceTab.toUpperCase().startsWith("#") && !sourceTab.toUpperCase().contains("RS-")) {
                        updateTableDetails(envMap, sourceTab, smutill, sysEnvTableDetails, tableColDetails);
                    }
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
                    if (!targetTab.toUpperCase().contains("RESULT") && !targetTab.toUpperCase().contains("INSERT") && !targetTab.toUpperCase().contains("UPDATE") && !targetTab.toUpperCase().startsWith("#") && !targetTab.toUpperCase().contains("RS-")) {
                        updateTableDetails(envMap, targetTab, smutill, sysEnvTableDetails, tableColDetails);
                    }
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

    public static void changeresultofTarget(ArrayList<MappingSpecificationRow> mapspeclist, String subjectName, String fromfileschemaName,String mapName) {
        try {
            Set<String> targettabset = new LinkedHashSet();
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                String srcStatus="false";
                String tgtStatus="false";
                MappingSpecificationRow row = iter.next();
                StringBuilder sourcetabsb = new StringBuilder();
                StringBuilder targettabsb = new StringBuilder();
                String targetTabName = row.getTargetTableName().replace("[", "").replace("]", "");
                String SourceTableName = row.getSourceTableName().replace("[", "").replace("]", "");
                String targetColumnName = row.getTargetColumnName().replace("[", "").replace("]", "");
                String sourceColumnName = row.getSourceColumnName().replace("[", "").replace("]", "");
                String[] Sourcetablenamearray = SourceTableName.split("\n");
                for (String Sourcetablename : Sourcetablenamearray) {
                    if (!Sourcetablename.toUpperCase().contains("RESULT") && !Sourcetablename.toUpperCase().contains("INSERT") && !Sourcetablename.toUpperCase().contains("UPDATE") && !Sourcetablename.toUpperCase().startsWith("#") && !Sourcetablename.toUpperCase().contains("RS-") && !Sourcetablename.toUpperCase().equalsIgnoreCase(subjectName)) {
                        if (!Sourcetablename.contains(".") && !"".equals(Sourcetablename)) {
                            if (StringUtils.isNotBlank(fromfileschemaName)) {
                                Sourcetablename = "SYS"+ "." + Sourcetablename;
                            }
                            sourcetabsb.append(Sourcetablename).append("\n");
                        }
                    }
                    if(Sourcetablename.toUpperCase().contains("#")){
                        
                        String value="RESULT_OF_";
                        srcStatus="true";
                        String mapName1="_"+mapName;
                        Sourcetablename  = Sourcetablename.replaceAll(value,"");
                        Sourcetablename  =Sourcetablename.replaceAll(mapName1,"");
                        Sourcetablename  = Sourcetablename.replaceAll("#", "");
                        //Sourcetablename  =Sourcetablename.substring(0, Sourcetablename.lastIndexOf("_"));
                        //Sourcetablename=Sourcetablename.substring(value.length(),mapName1.length());
                        SourceTableName=Sourcetablename; 
                        
                        
                    }
//                    
                }
                String[] targetTabNamearray = targetTabName.split("\n");
                for (String TargetTabName : targetTabNamearray) {
                    if (!TargetTabName.toUpperCase().contains("RESULT") && !TargetTabName.toUpperCase().contains("INSERT") && !TargetTabName.toUpperCase().contains("UPDATE") && !TargetTabName.toUpperCase().startsWith("#") && !TargetTabName.toUpperCase().contains("RS-") && !TargetTabName.toUpperCase().equalsIgnoreCase(subjectName)) {
                        if (!TargetTabName.contains(".") && !"".equals(TargetTabName)) {
                            if (StringUtils.isNotBlank(fromfileschemaName)) {
                                TargetTabName = "SYS" + "." + TargetTabName;
                            }
                            targettabsb.append(TargetTabName).append("\n");
                        }
                    }
                    if(TargetTabName.toUpperCase().contains("#")){
                        
                        String value="RESULT_OF_";
                        String mapName1="_"+mapName;
                        tgtStatus="true";
                        TargetTabName=TargetTabName.replaceAll(mapName1,"");
                        TargetTabName=TargetTabName.replaceAll(value,""); 
                        TargetTabName=TargetTabName.replaceAll("#","");
                        targetTabName=TargetTabName;
                        
                        
                       //TargetTabName=TargetTabName.substring(0,TargetTabName.lastIndexOf("_"));
                       //TargetTabName=TargetTabName.substring(value.length(),mapName1.length());
                       
                    }
                  
                }
                if(targetTabName.contains("RS") || targetTabName.contains("RESULT_OF_")){
                    String mapName1="_"+mapName;
                    targetTabName=targetTabName.replaceAll(mapName1,"");
                }
                if(SourceTableName.contains("RS") || SourceTableName.contains("RESULT_OF_")){
                    String mapName1="_"+mapName;
                    SourceTableName=SourceTableName.replaceAll(mapName1,"");
                }
                if (!sourcetabsb.toString().isEmpty()) {
                    row.setSourceTableName(sourcetabsb.toString().trim());
                } else {
                    row.setSourceTableName(SourceTableName);
                }
                if (!targettabsb.toString().isEmpty()) {
                    row.setTargetTableName(targettabsb.toString().trim());
                    extreamTargetTableName=targettabsb.toString().trim();
                    
                } else {
                    row.setTargetTableName(targetTabName);
                    extreamTargetTableName=targetTabName;
                }
                
//                if(srcStatus.equalsIgnoreCase("true")){
//                   row.setSourceTableName(SourceTableName); 
//                }
//                if(tgtStatus.equalsIgnoreCase("true")){
//                   row.setTargetTableName(targetTabName); 
//                }
                
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
            LOGGER.error("Exception Occured at Syncmetadata_Natixix_V2 inside addSourceColumnsDetails()>>", e);
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
            LOGGER.error("Exception Occured at Syncmetadata_Natixix_V2 inside addSourceColumnsDetails()>>", e);
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
            LOGGER.error("Exception Occured at Syncmetadata_Natixix_V2 inside addTargetColumnsDetails()>>", e);
        }

    }

    private static SMColumn getColDetails(String columnName, String tableName, Map<String, String> sysEnvTableDetails, Map<String, Map<String, Integer>> tableColDetails, Map<String, Map<Integer, SMColumn>> ColsDetails, List<String> sysEnvDetails, SystemManagerUtil smUtil) {
        if (!tableColDetails.containsKey(tableName)) {
            if (!tableName.toUpperCase().contains("RESULT") && !tableName.toUpperCase().contains("INSERT") && !tableName.toUpperCase().contains("UPDATE") && !tableName.toUpperCase().startsWith("#") && !tableName.toUpperCase().contains("RS-")) {
                updateTableDetails(sysEnvDetails, tableName, smUtil, sysEnvTableDetails, tableColDetails);
            }
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
                        colDataType.put(col.getColumnName()+"#"+sysEnvDetail[0]+"#"+sysEnvDetail[1],col.getColumnDatatype());
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

    public static void removeExtradots(ArrayList<MappingSpecificationRow> mapspeclist, String schema, List<String> envMap) {
        Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
        while (iter.hasNext()) {
            MappingSpecificationRow row = iter.next();
            String targetTabName = row.getTargetTableName().replace("[", "").replace("]", "");
            String SourceTableName = row.getSourceTableName().replace("[", "").replace("]", "");
            if (SourceTableName.startsWith(".")) {
                SourceTableName = SourceTableName.replace(".", "");
                row.setSourceTableName(SourceTableName);
            }
//            while (SourceTableName.contains("..")) {
//                SourceTableName = SourceTableName.replace("..", "." + schema + ".");
//            }
            if (targetTabName.startsWith(".")) {
                targetTabName = targetTabName.replace(".", "");
                 row.setTargetTableName(targetTabName);
            }
//            while (targetTabName.contains("..")) {
//                targetTabName = targetTabName.replace("..", "." + schema + ".");
//            }
           
        }

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

    public static void removestarcolumspec(List<MappingSpecificationRow> mapspeclist) {
        try {
            Map<String, String> starmap = new LinkedHashMap();
            Map<String, String> normalmap = new LinkedHashMap();
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row = iter.next();
                if (row.getSourceColumnName().equalsIgnoreCase("*")) {
                    starmap.put(row.getSourceTableName().toUpperCase().trim(), row.getSourceColumnName().toUpperCase().trim());
                } else {
                    normalmap.put(row.getSourceTableName().toUpperCase().trim(), row.getSourceColumnName().toUpperCase().trim());
                }
                if (row.getTargetTableName().equalsIgnoreCase("") && row.getTargetColumnName().equalsIgnoreCase("") && row.getBusinessRule().equalsIgnoreCase("")) {
                    iter.remove();
                }
            }
            forRemovingStar(mapspeclist, starmap, normalmap);
//             while (iter.hasNext()) {
//                MappingSpecificationRow row1 = iter.next();
//                if (row1.getSourceColumnName().equalsIgnoreCase("*")) {
//                    if(!normalmap.containsKey(row1.getSourceTableName().toUpperCase()))
//                    {
//                        if(starmap.containsKey(row1.getSourceTableName().toUpperCase()))
//                        {
//                            iter.remove();
//                        }
//                    }
//                }
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void forRemovingStar(List<MappingSpecificationRow> mapspeclist, Map<String, String> starmap, Map<String, String> normalmap) {
        try {
            Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
            while (iter.hasNext()) {
                MappingSpecificationRow row1 = iter.next();
                if (row1.getSourceColumnName().equalsIgnoreCase("*")) {
                    if (normalmap.containsKey(row1.getSourceTableName().toUpperCase())) {
                        if (starmap.containsKey(row1.getSourceTableName().toUpperCase())) {
                            iter.remove();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String preCreatingMapVersion(ArrayList<MappingSpecificationRow> mappingSpecificationRowsList, int projectId, String mappingName, KeyValueUtil keyValueUtil, MappingManagerUtil mappingManagerUtil, int subjectId) {
        Mapping latestMappingObj = null;
        ObjectMapper mapper = new ObjectMapper();
        String resultStatus = null;
        resultStatus = creatingMapVersion(projectId, mappingName, subjectId, mappingSpecificationRowsList, keyValueUtil, mappingManagerUtil);

        return resultStatus;

    }

    public static String creatingMapVersion(int projectId, String mappingName, int parentSubectId, ArrayList<MappingSpecificationRow> mapspecList, KeyValueUtil keyValueUtil, MappingManagerUtil mappingManagerUtil) {
        Mapping latestMappingObj = null;
        List<Float> latestMappingVersion = null;
        Float updateMappingVersion = 0.0f;
        RequestStatus resultStatus = null;
        StringBuilder statusbuilder = new StringBuilder();
        try {
            latestMappingVersion = getMappingVersions(parentSubectId, mappingName, mappingManagerUtil);
            updateMappingVersion = latestMappingVersion.get(latestMappingVersion.size() - 1);
            Mapping mappingObj = mappingManagerUtil.getMapping(parentSubectId, Node.NodeType.MM_SUBJECT, mappingName, updateMappingVersion);
            int mappId = mappingObj.getMappingId();
            mappingObj.setProjectId(projectId);
            mappingObj.setSubjectId(parentSubectId);
            mappingObj.setMappingId(mappId);
            mappingObj.setChangedDescription("Mapping " + mappingName + " changed! as Version Done: " + updateMappingVersion);
            String status = mappingManagerUtil.versionMapping(mappingObj).getStatusMessage();
            statusbuilder.append(mappingName + "--->" + status);
            List<Float> latestMapVersion = getMappingVersions(parentSubectId, mappingName, mappingManagerUtil);
            Float latestMapV = latestMapVersion.get(latestMapVersion.size() - 1);
            latestMappingObj = mappingManagerUtil.getMapping(parentSubectId, Node.NodeType.MM_SUBJECT, mappingName, latestMapV);
            int latestMapId = latestMappingObj.getMappingId();
            mappingManagerUtil.deleteMappingSpecifications(latestMapId);
            resultStatus = mappingManagerUtil.addMappingSpecifications(latestMapId, mapspecList);
            String msg = resultStatus.getStatusMessage();
            statusbuilder.append(msg + "--->" + status);
        } catch (Exception e) {
            StringWriter exceptionLog = new StringWriter();
            e.printStackTrace(new PrintWriter(exceptionLog));
        }
        return statusbuilder.toString();
    }

    public static List<Float> getMappingVersions(int subjectId, String mapName, MappingManagerUtil mappingManagerUtil) {
        List<Float> mapVersionList = new ArrayList();
        try {
            ArrayList<Mapping> mappings = mappingManagerUtil.getMappings(subjectId, Node.NodeType.MM_SUBJECT);

            if (!mappings.isEmpty()) {
                for (int map = 0; map < mappings.size(); map++) {
                    String mappingName = mappings.get(map).getMappingName();
                    float mappingVersion = mappings.get(map).getMappingSpecVersion();
                    if (mapName.equalsIgnoreCase(mappingName)) {
                        mapVersionList.add(mappingVersion);
                    }
                }
            }
        } catch (Exception e) {
            if ("Invalid ProjectId".equalsIgnoreCase(e.getMessage())) {
            }
        }
        return mapVersionList;
    }

    public static String addMapSpecificationsForFullLoadType(int mapId, ArrayList<MappingSpecificationRow> mappingSpecificationRowsList, MappingManagerUtil mappingManagerUtil) {

        RequestStatus resultStatus = null;

        resultStatus = mappingManagerUtil.deleteMappingSpecifications(mapId);
        resultStatus = mappingManagerUtil.addMappingSpecifications(mapId, mappingSpecificationRowsList);
        return resultStatus.getStatusMessage();
    }
    public static String gettingdatabaseNameFromQueryUsingUseWord(String query) {
        String databaseName = "";
        String pattern = "USE";
        if (query.toUpperCase().contains("USE")) {
            try {
                int i = 0;
                String[] linearr = query.split("\n");
                for (String line : linearr) {
                    line = line.replaceAll("\\ + ", "\\ ").trim();
                    if (line.contains("USE")) {
                        int index = line.indexOf("USE");
                        if (line.indexOf(" ", index + pattern.length() + 1) != -1) {
                            line = line.substring(index + pattern.length() + 1, line.indexOf(" ", index + pattern.length() + 1));
                        } else {
                            line = line.substring(line.indexOf(" ", index + pattern.length()));
                        }
                        line = line.replace("[", "").replace("]", "");
                        databaseName = line.toUpperCase().trim();
                    }
                    if(StringUtils.isNotEmpty(databaseName))
                    {
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return databaseName;
    }

    private static void metadataDesign(List<Mapping> mapObj) {
        ArrayList<MappingSpecificationRow> mapSPecsLists = mapObj.get(0).getMappingSpecifications();
        for(MappingSpecificationRow row:mapSPecsLists){
            String srcsysName=row.getSourceSystemName();
            String srcenvName=row.getSourceSystemEnvironmentName();
            
            String tgtsysName=row.getTargetSystemName();
            String tgtenvName=row.getTargetSystemEnvironmentName();
            String tgtTableName=row.getTargetTableName();
            if(srcsysName=="" || srcenvName== "" || srcsysName.trim().length()==0){
                srcsysName="Oracle";
                srcenvName="Sys";
                row.setSourceSystemName(srcsysName);
                row.setSourceSystemEnvironmentName(srcenvName);
            }
            if(tgtsysName=="" || tgtenvName== "" || tgtsysName.trim().length()==0){
                tgtsysName="Oracle";
                tgtenvName="Sys";
                row.setTargetSystemName(tgtsysName);
                row.setTargetSystemEnvironmentName(tgtenvName);
            }
            
            if(tgtTableName.toUpperCase().contains(extreamTargetTableName.toUpperCase())&&tgtTableName.toUpperCase().contains("RS")){
                row.setTargetTableName(mapObj.get(0).getMappingName());
            }
        }
    }
    
    
     public static String addMappingSpecDoc1(MappingManagerUtil mappingManagerUtil,String mapName,int mapID,String filePath) {
         File f1=new File(filePath);
         String type=f1.getName().split("\\.")[1];
        Document mapDoc = new Document();
        mapDoc.setDocumentName(mapName);
        mapDoc.setDocumentType(type);
        mapDoc.setDocumentIntendedPurpose("Query Extract");
        mapDoc.setDocumentOwner("Admin");
        mapDoc.setDocumentObject(filePath);
        mapDoc.setDocumentLink(" ");
        mapDoc.setDocumentReference(" ");
        mapDoc.setApprovalRequired(true);
        Date today = new Date();
        mapDoc.setDocumentApprovedDate(today);
        mapDoc.setDocumentStatus("Approved");

        RequestStatus req = mappingManagerUtil.addMappingDocument(mapID,mapDoc);
        return req.getStatusMessage();
    }

    private static void modificationOfExtreamtarget(List<Mapping> mapObj,String mapName) {
       ArrayList<MappingSpecificationRow> mapRow= mapObj.get(0).getMappingSpecifications();
       String FinalTableName="";
       Set<String> tableName=ExtreamSourceAndExtreamTarget.getExtremeTargetSet(mapRow);
       for(String tableName1 :tableName){
           FinalTableName=tableName1;
       }
       for(MappingSpecificationRow row :mapRow){
           String targetTableName=row.getTargetTableName();
           
           if(FinalTableName.toUpperCase().equalsIgnoreCase(targetTableName.toUpperCase())){
               row.setTargetTableName(mapName);
           }
       }
       
    }

    private static void removeDupilcateMapSpecRow(List<Mapping> mapObj) {
        ArrayList<MappingSpecificationRow> mapRow=mapObj.get(0).getMappingSpecifications();
        String sqlText=mapObj.get(0).getSourceExtractQuery();
        ArrayList<MappingSpecificationRow> mapSpecRows=new ArrayList<>();
        LinkedHashSet<String> mappingspec=new LinkedHashSet<>();
        for(MappingSpecificationRow e : mapRow ){
            String srcSystemName   =e.getSourceSystemName().toUpperCase().trim();
            String srcEnvName   =e.getSourceSystemEnvironmentName().toUpperCase().trim();
            String sourceTableName =e.getSourceTableName().toUpperCase().trim();
            String sourceColumnName=e.getSourceColumnName().toUpperCase().trim();
            
            String tgtSystemName   =e.getTargetSystemName().toUpperCase().trim();
            String tgtEnvName   =e.getTargetSystemEnvironmentName().toUpperCase().trim();
            String targetTableName =e.getTargetTableName().toUpperCase().trim();
            String targetColumnName=e.getTargetColumnName().toUpperCase().trim();
            String businessRule=e.getBusinessRule().toUpperCase().trim();
            if(businessRule!=""){
             mappingspec.add(srcSystemName+"#"+srcEnvName+"#"+sourceTableName+"#"+sourceColumnName+"#"+tgtSystemName+"#"+tgtEnvName+"#"+targetTableName+"#"+targetColumnName+"#"+businessRule);    
            } else{
                mappingspec.add(srcSystemName+"#"+srcEnvName+"#"+sourceTableName+"#"+sourceColumnName+"#"+tgtSystemName+"#"+tgtEnvName+"#"+targetTableName+"#"+targetColumnName); 
            } 
        }
        
        for(String name:mappingspec){
            MappingSpecificationRow mappingSpecificationRow=new MappingSpecificationRow();
            mappingSpecificationRow.setSourceSystemName(name.split("\\#")[0]);
            mappingSpecificationRow.setSourceSystemEnvironmentName(name.split("\\#")[1]);
            mappingSpecificationRow.setSourceTableName(name.split("\\#")[2]);
            String sourceColumnName=name.split("\\#")[3];
            String sourceColumnDt="";
            if(colDataType.containsKey(sourceColumnName.trim()+"#"+name.split("\\#")[0]+"#"+name.split("\\#")[1])){
                sourceColumnDt=colDataType.get(sourceColumnName.trim()+"#"+name.split("\\#")[0]+"#"+name.split("\\#")[1]);
                mappingSpecificationRow.setSourceColumnName(name.split("\\#")[3]);
                mappingSpecificationRow.setSourceColumnDatatype(sourceColumnDt);
            }else{
                mappingSpecificationRow.setSourceColumnName(name.split("\\#")[3]);
            }
            
            
            mappingSpecificationRow.setTargetSystemName(name.split("\\#")[4]);
            mappingSpecificationRow.setTargetSystemEnvironmentName(name.split("\\#")[5]);
            mappingSpecificationRow.setTargetTableName(name.split("\\#")[6]);
            //mappingSpecificationRow.setTargetColumnName(name.split("\\#")[7]);
            String tgtColumnName=name.split("\\#")[7];
            String tgtColumnDt="";
            if(colDataType.containsKey(tgtColumnName.trim()+"#"+name.split("\\#")[4]+"#"+name.split("\\#")[5])){
               tgtColumnDt=colDataType.get(tgtColumnName.trim()+"#"+name.split("\\#")[4]+"#"+name.split("\\#")[5]);
                mappingSpecificationRow.setTargetColumnName(name.split("\\#")[7]);
                mappingSpecificationRow.setTargetColumnDatatype(tgtColumnDt);
            }else{
                mappingSpecificationRow.setTargetColumnName(name.split("\\#")[7]);
            }
            try{
              mappingSpecificationRow.setBusinessRule(name.split("\\#")[8]); 
              mapSpecRows.add(mappingSpecificationRow);
            }catch(Exception e){
                mapSpecRows.add(mappingSpecificationRow);
            }
           
        }
        Mapping map=new Mapping();
        map.setMappingSpecifications(mapSpecRows);
        map.setSourceExtractQuery(sqlText);
        mapObj.clear();
        mapObj.add(map);
    }

    private static List<String> getAllEnviroment(String jsonMetadatapath,String folder) {
       List<String> details=new ArrayList<>();
        try{
            File value=new File(jsonMetadatapath+folder);
            File[] fileList=value.listFiles();
            
            for(int i=0;i<fileList.length;i++){
                String fileName=fileList[i].getName();
                fileName=fileName.replaceAll(".json","");
                if(fileName.contains("__")){
                  String systemName=fileName.split("\\__")[0];
                String envName=fileName.split("\\__")[1];
                
                details.add(systemName+"###"+envName);   
                }
               
            }
            
        }catch(Exception e){
          e.printStackTrace();
        }
        return details;
    }
    
    public static void moveFileToArchive(String archivepath,String sourcepath){
       File arcFile=new File(archivepath);
       File srcFile=new File(sourcepath);
       File[] arr=srcFile.listFiles();
       
       for(int i=0;i<arr.length;i++){
           try {
               File createFile=new File(archivepath+"\\"+arr[i].getName());
               createFile.createNewFile();
               FileUtils.copyFile(arr[i].getAbsoluteFile(),createFile);
           } catch (IOException ex) {
               ex.printStackTrace();
               java.util.logging.Logger.getLogger(Syncmetadata_AF_V2_Oracle.class.getName()).log(Level.SEVERE, null, ex);
           }
       }
    }
    public static void deleteFile(String filepath){
        File delFile=new File(filepath);
        File[] arr=delFile.listFiles();
        
        for(int i=0;i<arr.length;i++){
            File f1=new File(arr[i].getAbsolutePath());
            f1.delete();
        }
        
    }
    public static void inputOptionDetails(HashMap<String,String> details, StringBuilder logger){
         int keyLength=30;
          logger.append("Log Status").append("\n").append("----------------").append("\n").append("Input Parameters").append("\n").append("----------------").append("\n");
         try{
             for (Map.Entry<String,String> entry : details.entrySet()){
                 String inputOP= entry.getKey(); 
                 String value= entry.getValue();
                 
                 logger.append(inputOP+"    ="+value);
                 logger.append("\n");
             } 
            
                     logger.append("----------------").append("\n").append("Execution Status").append("\n").append("----------------").append("\n"); 
         }catch(Exception e){
             e.printStackTrace();
         }
         
     }
    public static void mappingCreation(StringBuffer sb, StringBuilder logger){
          String[] stArray=sb.toString().split("\n");
         logger.append(stArray.length).append(" ").append(stArray[0]).append("\n");
     }
    
     public static void timeStamp(long startTime, long endTime, StringBuilder logger){
        try {
            logger.append("----------------").append("\n").append(" ").append("\n").append("----------------").append("\n");
            
            SimpleDateFormat simple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            
            // Creating date from milliseconds
            // using Date() constructor
            Date stresult = new Date(startTime);
            Date endresult = new Date(endTime);
            
            String stdate1=simple.format(stresult);
            String enDate1=simple.format(endresult);
            
            Date d1=null;
            Date d2=null;
            
            d1=simple.parse(stdate1);
            d2=simple.parse(enDate1);
            
            long diff = d2.getTime() - d1.getTime();
            long diffSeconds = diff / 1000;
            long diffMinutes = diff / (60 * 1000);
            long diffHours = diff / (60 * 60 * 1000);
            
            SimpleDateFormat sec = new SimpleDateFormat("ss");
            SimpleDateFormat min = new SimpleDateFormat("mm");
            SimpleDateFormat hr = new  SimpleDateFormat ("hh");
            
            String sec1=Long.toString(diffSeconds);
            String min1=Long.toString(diffMinutes);
            String hr1=Long.toString(diffHours);
            
            logger.append("Start Time           :"+" ").append(stdate1).append("\n");
            logger.append("End Time             :"+" ").append(enDate1).append("\n");
            
            
            //logger.append("Total Time           :"+" ").append(sec1+" " ).append("\n");
        } catch (ParseException ex) {
            java.util.logging.Logger.getLogger(Syncmetadata_AF_V2_Oracle.class.getName()).log(Level.SEVERE, null, ex);
        }
            
     }
     public static String modifyFilePath(String filePath){
         try{
            if(filePath.endsWith("\\")){
             return filePath.substring(0,filePath.length()-1);
            } 
         }catch(Exception e){
             e.printStackTrace();
         }
         return  filePath;
     }
     
     public static String getLogTimeStamp(){
     long endValue=System.currentTimeMillis();
     SimpleDateFormat simple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            
            Date endresult = new Date(endValue);
            
            String stdate1=simple.format(endresult);
            stdate1=stdate1.replaceAll("[^a-zA-Z0-9]", " ");
            stdate1=stdate1.replaceAll(" ","");
        return "_"+stdate1;
    }
}

