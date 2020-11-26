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
import org.codehaus.jackson.type.TypeReference;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;
import com.ads.api.util.SystemManagerUtil;
import static com.erwin.metadata_zovio_af.Syncmetadata.changeresultofSource;
import static com.erwin.metadata_zovio_af.Syncmetadata.changeresultofTarget;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author InkolluReddy
 */


public class SyncMetadataSsas {

    public static String metadataSync(Map<String, String> envMap, SystemManagerUtil smutill, String json) {
        String jsonvalue = "";
        try {
            ObjectMapper mapper = new ObjectMapper();
            json = json.replace(",\"childNodes\":[]", "");
            List<Mapping> mapObj = (List<Mapping>) mapper.readValue(json, (TypeReference) new TypeReference<List<Mapping>>() {
            });
            ArrayList<MappingSpecificationRow> mapSPecsLists = (ArrayList<MappingSpecificationRow>) mapObj.get(0).getMappingSpecifications();
            envMap.toString();
            String mapName = mapObj.get(0).getMappingName();
            String storproc = mapName;
            String storprocName = storproc.replaceAll("[0-9]", "");
            if (mapName.contains(".")) {
                storproc = mapName.substring(0, mapName.lastIndexOf("."));
                storprocName = storproc.replaceAll("[0-9]", "");
            }
            changeresultofTarget(mapSPecsLists);
            for (MappingSpecificationRow mapSPecs : mapSPecsLists) {
                envMap.toString();

                String Sourcetablename = mapSPecs.getSourceTableName();
                sourceNamesSet(Sourcetablename.trim(), envMap, mapSPecs, mapName, storprocName);
                String targetTableName = mapSPecs.getTargetTableName();

                String[] targetval = targetTableName.split("\n");
                StringBuilder systemsb = new StringBuilder();
                StringBuilder envsb = new StringBuilder();
                for (String tgtTabName : targetval) {
                    //  if (tgtName.contains(".")) {
                    //    tgtName = tgtName.split("\\.")[1];
                    // }
                    targetNameSet(tgtTabName, envMap, mapSPecs, mapName, storprocName);
                }
            }
            changeresplitTable(mapSPecsLists);
            String mapjson = mapper.writeValueAsString((Object) mapObj);
            return mapjson;
        } catch (Exception ex) {
            Logger.getLogger(SyncmetadataPWC.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    public static void sourceNamesSet(String Sourcetablename, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName) {
        if (Sourcetablename.split("\n").length > 1) {
            String[] sourcetablecolumn = Sourcetablename.split("\n");
            List<String> sourcesystem = getSourceSystem(sourcetablecolumn, mapName, envMap, mapSPecs, storprocName);
            List<String> sourceenv = getSourceEnv(sourcetablecolumn, mapName, envMap, mapSPecs, storprocName);
            List<String> sourcetab = getSourcetab(sourcetablecolumn, mapName, envMap, storprocName);
            String sourceSystem = StringUtils.join((Iterable) sourcesystem, "\n");
            String sourceEnv = StringUtils.join((Iterable) sourceenv, "\n");
            String sourceTab = StringUtils.join((Iterable) sourcetab, "\n");
            mapSPecs.setSourceSystemName(sourceSystem);
            mapSPecs.setSourceSystemEnvironmentName(sourceEnv);
            mapSPecs.setSourceTableName(sourceTab);
            envMap.toString();
        }

        if (envMap.containsKey(Sourcetablename.toUpperCase())) {
            String sourceenvSys = envMap.get(Sourcetablename.toUpperCase());
            String SystemName = sourceenvSys.split("#")[1];
            String environmentName = sourceenvSys.split("#")[0];
            String schemaname = "";
            if (sourceenvSys.split("#").length == 3) {
                schemaname = sourceenvSys.split("#")[2];
            }
            if (Sourcetablename.split("\\.").length == 3) {
                environmentName = Sourcetablename.split("\\.")[0];
            }
            if (!"".equals(schemaname)) {
                mapSPecs.setSourceTableName(schemaname + "." + Sourcetablename);
            }
            mapSPecs.setSourceSystemName(SystemName);
            mapSPecs.setSourceSystemEnvironmentName(environmentName);
        } 
        if (Sourcetablename.toUpperCase().contains("RESULT_OF_") || Sourcetablename.toUpperCase().contains("INSERT-SELECT")||Sourcetablename.toUpperCase().contains("UPDATE-SELECT")) {
                mapSPecs.setSourceTableName(Sourcetablename + "_" + mapName);
            }
     
    }

    public static void targetNameSet(String targetTableName, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName) {

        try {
            if (envMap.containsKey(targetTableName.toUpperCase())) {
                String targetenvSys = envMap.get(targetTableName.toUpperCase());
                String SystemName = targetenvSys.split("#")[1];
                mapSPecs.setTargetSystemName(SystemName);
                String environmentName = targetenvSys.split("#")[0];
                mapSPecs.setTargetSystemEnvironmentName(environmentName);
                String schemaname = "";
                if (targetenvSys.split("#").length == 3) {
                    schemaname = targetenvSys.split("#")[2];
                }
                if (!"".equals(schemaname)) {
                    mapSPecs.setTargetTableName(schemaname + "." + targetTableName);
                }
            }
            if (targetTableName.toUpperCase().contains("RESULT_OF_") || targetTableName.toUpperCase().contains("INSERT-SELECT")||targetTableName.toUpperCase().contains("UPDATE-SELECT")) {
                mapSPecs.setSourceTableName(targetTableName + "_" + mapName);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static List<String> getSourceSystem(String[] sourcetablename, String mapname, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String storprocName) {
        List<String> sourcesystem = new LinkedList<String>();
        String systemName = "";
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);
                    sourcesystem.add(sourceenvSys.split("#")[1]);
                } else {
                    if (sourceTab.split("\\.").length == 3) {
                        systemName = sourceTab.split("\\.")[1];
                        if (envMap.containsKey(systemName.toUpperCase())) {
                            String envstr = envMap.get(systemName.toUpperCase());
                            systemName = envstr.split("#")[1];
                            sourcesystem.add(systemName);
                        }

                    }else{
                    sourcesystem.add(mapSPecs.getSourceSystemName());
                    }
                    
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourcesystem;
    }

    public static List<String> getSourceEnv(String[] sourcetablename, String mapname, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String storprocName) {
        List<String> sourceenv = new LinkedList<String>();
        String envName="";
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);
                    sourceenv.add(sourceenvSys.split("#")[0]);
                } else {
                   if (sourceTab.split("\\.").length == 3) {
                        envName = sourceTab.split("\\.")[1];
                        if (envMap.containsKey(envName.toUpperCase())) {
                            String envstr = envMap.get(envName.toUpperCase());
                            envName = envstr.split("#")[1];
                            sourceenv.add(envName);
                        }

                    }else{
                    sourceenv.add(mapSPecs.getSourceSystemEnvironmentName());
                    }
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
                sourcetableName.add(sourceTab);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourcetableName;
    }

    public static void changeresultofTarget(ArrayList<MappingSpecificationRow> mapspeclist) {
        Set<String> targettabset = new LinkedHashSet();
        Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();

        while (iter.hasNext()) {
            MappingSpecificationRow row = iter.next();
            String targetTabName = row.getTargetTableName().replace("[", "").replace("]", "");
            String SourceTableName = row.getSourceTableName().replace("[", "").replace("]", "");
            String targetColumnName = row.getTargetColumnName().replace("[", "").replace("]", "");
            String sourceColumnName = row.getSourceColumnName().replace("[", "").replace("]", "");
            if (!SourceTableName.toUpperCase().contains("RESULT") && !SourceTableName.toUpperCase().contains("INSERT") && !SourceTableName.toUpperCase().contains("UPDATE")) {
                if (!SourceTableName.contains(".") && !"".equals(SourceTableName)) {

                    SourceTableName = "DBO." + SourceTableName;
                }

            }
            if (!targetTabName.toUpperCase().contains("RESULT") && !targetTabName.toUpperCase().contains("INSERT") && !targetTabName.toUpperCase().contains("UPDATE")) {
                if (!targetTabName.contains(".") && !"".equals(targetTabName)) {
                    targetTabName = "DBO." + targetTabName;
                }
            }

            row.setSourceTableName(SourceTableName);
            row.setTargetTableName(targetTabName);
            row.setSourceColumnName(sourceColumnName);
            row.setTargetColumnName(targetColumnName);
        }
    }

    public static void changeresplitTable(ArrayList<MappingSpecificationRow> mapspeclist) {
        Set<String> targettabset = new LinkedHashSet();
        Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();

        while (iter.hasNext()) {
            MappingSpecificationRow row = iter.next();
            String targetTabName = row.getTargetTableName();
            String SourceTableName = row.getSourceTableName();

            if (SourceTableName.split("\\.").length == 3) {

                SourceTableName = SourceTableName.split("\\.")[1] + "." + SourceTableName.split("\\.")[2];
            }
            if (targetTabName.trim().split("\n").length > 1) {

            } else {
                if (targetTabName.split("\\.").length == 3) {

                    targetTabName = targetTabName.split("\\.")[1] + "." + targetTabName.split("\\.")[2];
                }

            }

            row.setSourceTableName(SourceTableName);
            row.setTargetTableName(targetTabName);

        }
    }

}
