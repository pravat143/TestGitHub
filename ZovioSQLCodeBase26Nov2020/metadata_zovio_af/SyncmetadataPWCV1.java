// 
// Decompiled by Procyon v0.5.36
// 
package com.erwin.metadata_zovio_af;

import java.util.LinkedList;
import com.ads.api.beans.sm.SMTable;
import com.ads.api.beans.sm.SMEnvironment;
import com.ads.api.beans.sm.SMSystem; 
import java.util.HashMap;
import org.apache.commons.lang3.StringUtils;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.sm.SMColumn;
import org.codehaus.jackson.type.TypeReference;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;
import com.ads.api.util.SystemManagerUtil;
import com.fasterxml.jackson.databind.introspect.POJOPropertyBuilder;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class SyncmetadataPWCV1 {

    public static SystemManagerUtil smutil = null;

    public static String metadataSync(Map<String, String> envMap, SystemManagerUtil smutill, String json) {
        String jsonvalue = "";
        try {
            smutil = smutill;
            ObjectMapper mapper = new ObjectMapper();
            json = json.replace(",\"childNodes\":[]", "");
            List<Mapping> mapObj = (List<Mapping>) mapper.readValue(json, (TypeReference) new TypeReference<List<Mapping>>() {
            });
            ArrayList<MappingSpecificationRow> mapSPecsLists = (ArrayList<MappingSpecificationRow>) mapObj.get(0).getMappingSpecifications();
            envMap.toString();
            String mapName = mapObj.get(0).getMappingName();
            String storproc = mapName.substring(0, mapName.lastIndexOf("."));
            String storprocName = storproc.replaceAll("[0-9]", "");
            for (MappingSpecificationRow mapSPecs : mapSPecsLists) {
                String Sourcetablename = mapSPecs.getSourceTableName();
                if (Sourcetablename.contains(".")) {
                    Sourcetablename = Sourcetablename.split("\\.")[1];
                }
                sourceNamesSet(Sourcetablename, envMap, mapSPecs, mapName, storprocName);
                String Targettablename = mapSPecs.getTargetTableName();
                String[] split;
                String[] trgtname = split = Targettablename.split("\n");
                for (String tgtName : split) {
                    if (tgtName.contains(".")) {
                        tgtName = tgtName.split("\\.")[1];
                    }
                    targetNameSet(tgtName, envMap, mapSPecs, mapName, storprocName);
                }
            }
            removeNullfromspec(mapSPecsLists);
            changeTargettableName(mapSPecsLists);
            String mapjson = mapper.writeValueAsString((Object) mapObj);
            return mapjson;
        } catch (Exception ex) {
            Logger.getLogger(SyncmetadataPWCV1.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    public static void sourceNamesSet(String Sourcetablename, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName) {
        if (Sourcetablename.split("\n").length > 1) {
            String[] sourcetablecolumn = Sourcetablename.split("\n");
            List<String> sourcesystem = getSourceSystem(sourcetablecolumn, mapName, envMap, mapSPecs, storprocName);
            List<String> sourceenv = getSourceEnv(sourcetablecolumn, mapName, envMap, storprocName);
            List<String> sourcetab = getSourcetab(sourcetablecolumn, mapName, envMap, storprocName);
            String sourceSystem = StringUtils.join((Iterable) sourcesystem, "\n");
            String sourceEnv = StringUtils.join((Iterable) sourceenv, "\n");
            String sourceTab = StringUtils.join((Iterable) sourcetab, "\n");
            mapSPecs.setSourceSystemEnvironmentName(sourceEnv);
            mapSPecs.setSourceSystemName(sourceSystem);
            mapSPecs.setSourceTableName(sourceTab);
        } else if (envMap.containsKey(Sourcetablename.toUpperCase())) {
            String sourceenvSys = envMap.get(Sourcetablename.toUpperCase());
            String SystemName = sourceenvSys.split("#")[1];
            String environmentName = sourceenvSys.split("#")[0];
            String schemaname = "";
            if (sourceenvSys.split("#").length == 3) {
                schemaname = sourceenvSys.split("#")[2];
            }
            if (!"".equals(schemaname)) {
                mapSPecs.setSourceTableName(schemaname + "." + Sourcetablename);
            }
            mapSPecs.setSourceSystemName(SystemName);
            mapSPecs.setSourceSystemEnvironmentName(environmentName);
        }
    }

    public static void targetNameSet(String Targettablename, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName) {
        if (envMap.containsKey(Targettablename.toUpperCase())) {
            String targetenvSys = envMap.get(Targettablename.toUpperCase());
            String SystemName = targetenvSys.split("#")[1];
            mapSPecs.setTargetSystemName(SystemName);
            String environmentName = targetenvSys.split("#")[0];
            mapSPecs.setTargetSystemEnvironmentName(environmentName);
            String schemaname = "";
            if (targetenvSys.split("#").length == 3) {
                schemaname = targetenvSys.split("#")[2];
            }
            if (!"".equals(schemaname)) {
                mapSPecs.setTargetTableName(schemaname + "." + Targettablename);
            }
        }
    }

    public static Map<String, List<String>> metaDatacreation(SystemManagerUtil smutill, String targetTableName) {
        Map<String, String> metaDatacreationmap = new HashMap<String, String>();
        Map<String, List<String>> tableMap = new HashMap();
        try {
            ArrayList<SMSystem> systems = (ArrayList<SMSystem>) smutill.getSystems();
            for (int i = 0; i < systems.size(); ++i) {
                String sysName = systems.get(i).getSystemName();
                int sysid = systems.get(i).getSystemId();
                ArrayList<SMEnvironment> environments = (ArrayList<SMEnvironment>) smutill.getEnvironments(sysid);
                for (int j = 0; j < environments.size(); ++j) {
                    int envid = environments.get(j).getEnvironmentId();
                    SMEnvironment environment = smutill.getEnvironment(envid);
                    String envName = environment.getSystemEnvironmentName();
                    SMEnvironment smEnv = smutill.getEnvironment(envid, true);
                    List<SMTable> envtables = (List<SMTable>) smutill.getEnvironmentTables(envid);
                    for (int k = 0; k < envtables.size(); ++k) {
                        String tableName = envtables.get(k).getTableName();

                        int tableid = envtables.get(k).getTableId();

                        if (targetTableName.equalsIgnoreCase(tableName)) {
                            List<String> columnstrlist = new LinkedList();
                            List<SMColumn> columnlist = smutill.getColumns(tableid);
                            for (SMColumn sMColumn : columnlist) {
                                String columnName = sMColumn.getColumnName();
                                columnstrlist.add(columnName);
                            }
                            tableMap.put(tableName, columnstrlist);
                        }
                        String envsys = envName + "#" + sysName;
                        metaDatacreationmap.put(tableName.toUpperCase(), envsys);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tableMap;
    }

    public static int setsrcSysenvNames(String src_table_name, String mapName) {
        int skipFlag = 0;
        String Sourcetablename = src_table_name.toLowerCase();
        String schemaNm = "";
        if (Sourcetablename.contains(".")) {
            schemaNm = Sourcetablename.split("[.]")[0];
            Sourcetablename = Sourcetablename.split("[.]")[1] + "load";
        }
        if (!"dbo".equals(schemaNm) && !"".equals(schemaNm)) {
            Sourcetablename = schemaNm + "_" + Sourcetablename;
        }
        if (mapName.toLowerCase().contains(Sourcetablename)) {
            skipFlag = 1;
        }
        return skipFlag;
    }

    public static Mapping createMapFromMappingSpecifiactionRow(ArrayList<MappingSpecificationRow> specrowlist, String mapfileName, String query, int projectid, int subjectid) {
        Mapping mapping = new Mapping();
        mapping.setMappingName(mapfileName);
        mapping.setMappingSpecifications((ArrayList) specrowlist);
        mapping.setSourceExtractQuery(query);
        mapping.setProjectId(projectid);
        mapping.setSubjectId(subjectid);
        return mapping;
    }

    public static List<String> getSourceSystem(String[] sourcetablename, String mapname, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String storprocName) {
        List<String> sourcesystem = new LinkedList<String>();
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);
                    sourcesystem.add(sourceenvSys.split("#")[1]);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourcesystem;
    }

    public static List<String> getSourceEnv(String[] sourcetablename, String mapname, Map<String, String> envMap, String storprocName) {
        List<String> sourceenv = new LinkedList<String>();
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);
                    sourceenv.add(sourceenvSys.split("#")[0]);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceenv;
    }

    public static List<String> getSourcetab(String[] sourcetablename, String mapname, Map<String, String> envMap, String storprocName) {
        List<String> sourcetableName = new LinkedList<String>();
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);
                    if (sourceenvSys.split("#").length == 3) {
                        sourcetableName.add(sourceenvSys.split("#")[2] + "." + sourceTab);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourcetableName;
    }

    public static void changeTargettableName(ArrayList<MappingSpecificationRow> maprow) {

        try {
            List<String> targetTableString = new LinkedList();
            for (MappingSpecificationRow mappingSpecificationRow : maprow) {

                String targetTabString = mappingSpecificationRow.getTargetTableName();
                if (targetTabString.toUpperCase().contains("RESULT") || targetTabString.toUpperCase().contains("INSERT-SELECT") || "".equals(targetTabString)) {
                    continue;
                }
                if (!targetTableString.contains(targetTabString)) {
                    targetTableString.add(targetTabString);

                    String targetColumnName = mappingSpecificationRow.getTargetColumnName();
                    Map<String, List<String>> tablemap = metaDatacreation(smutil, targetTabString);

                    changeTarget(maprow, targetTabString);
                    Map<String, String> targetMap = getTargetColumnMap(maprow, targetTabString, tablemap.get(targetTabString.toUpperCase()));
                    changeTargetCOlumn(maprow, targetTabString, targetMap);
                }

            }

        } catch (Exception e) {

        }

    }

    public static void changeTarget(ArrayList<MappingSpecificationRow> maprow, String targetTab) {

        try {

            for (MappingSpecificationRow mappingSpecificationRow : maprow) {

                if (targetTab.equalsIgnoreCase(mappingSpecificationRow.getTargetTableName())) {
                    if ("".equals(mappingSpecificationRow.getBusinessRule())) {
                        mappingSpecificationRow.setTargetColumnName("");
                    }
                }

            }

        } catch (Exception e) {

        }

    }

    public static void changeTargetCOlumn(ArrayList<MappingSpecificationRow> maprow, String targetTab, Map<String, String> targetmetadat) {

        try {

            for (MappingSpecificationRow mappingSpecificationRow : maprow) {

                if (targetTab.equalsIgnoreCase(mappingSpecificationRow.getTargetTableName())) {
                    if (!"".equals(mappingSpecificationRow.getBusinessRule())) {
                        String targetCOlumnName = mappingSpecificationRow.getTargetColumnName();
                        String metadatacolumn = targetmetadat.get(targetCOlumnName);
                        mappingSpecificationRow.setTargetColumnName(metadatacolumn);
                    }

                }

            }

        } catch (Exception e) {

        }

    }

    public static void removeNullfromspec(ArrayList<MappingSpecificationRow> mapspeclist) {
        Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
        while (iter.hasNext()) {
            MappingSpecificationRow row = iter.next();
            if (row.getTargetColumnName().equals("") || row.getSourceColumnName().equals("")) {
                iter.remove();
            }
        }
    }
    
    public static void changeresultofTarget(ArrayList<MappingSpecificationRow> mapspeclist,String mapname) {
         Set<String> targettabset = new LinkedHashSet();
        Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
       
        while (iter.hasNext()) {
            MappingSpecificationRow row = iter.next();
            String targetTabName = row.getTargetTableName();
            String targetarr[] = targetTabName.split("\n");
            for (String targettab : targetarr) {
                if(targettab.toUpperCase().contains("RESULT_OF")){
                
                targettabset.add(targettab.toUpperCase()+"_"+mapname);
                }
            }
            String appendtargetTabName = StringUtils.join(targettabset,"\n");
            row.setTargetTableName(appendtargetTabName);  
        }
    }
     public static void changeresultofSource(ArrayList<MappingSpecificationRow> mapspeclist,String mapname) {
         Set<String> targettabset = new LinkedHashSet();
        Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
       
        while (iter.hasNext()) {
            MappingSpecificationRow row = iter.next();
            String sourceTabName = row.getSourceTableName();
            String sourcearr[] = sourceTabName.split("\n");
            for (String sourcetab : sourcearr) {
                if(sourcetab.toUpperCase().contains("RESULT_OF")){
                
                targettabset.add(sourcetab.toUpperCase()+"_"+mapname);
                }
            }
            String appendsource = StringUtils.join(targettabset,"\n");
            row.setSourceTableName(appendsource);  
        }
    }

    public static Map<String, String> getTargetColumnMap(ArrayList<MappingSpecificationRow> maprow, String targetTab, List<String> columnList) {
        Map<String, String> targetmap = new LinkedHashMap();
        try {

            for (MappingSpecificationRow mappingSpecificationRow : maprow) {
                for (String columnStr : columnList) {
                    if (targetTab.equalsIgnoreCase(mappingSpecificationRow.getTargetTableName())) {
                        targetmap.put(mappingSpecificationRow.getSourceColumnName(), columnStr);
                        mappingSpecificationRow.setTargetColumnName(columnStr);
                        columnList.remove(columnStr);
                        break;

                    }

                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return targetmap;
    }
}
