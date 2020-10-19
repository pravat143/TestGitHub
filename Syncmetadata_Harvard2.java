// 
// Decompiled by Procyon v0.5.36
// 

package com.erwin.metadata;

import java.util.LinkedList;
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
import java.util.Map;

public class Syncmetadata_Harvard2
{
    public static String metadataSync(final Map<String, String> envMap, final SystemManagerUtil smutill, String json, final String folderName) {
        final String jsonvalue = "";
        try {
            final ObjectMapper mapper = new ObjectMapper();
            json = json.replace(",\"childNodes\":[]", "");
            final List<Mapping> mapObj = (List<Mapping>)mapper.readValue(json, (TypeReference)new TypeReference<List<Mapping>>() {});
            final ArrayList<MappingSpecificationRow> mapSPecsLists = (ArrayList<MappingSpecificationRow>)mapObj.get(0).getMappingSpecifications();
            envMap.toString();
            String storproc;
            final String mapName = storproc = mapObj.get(0).getMappingName();
            String storprocName = storproc.replaceAll("[0-9]", "");
            if (mapName.contains(".")) {
                storproc = mapName.substring(0, mapName.lastIndexOf("."));
                storprocName = storproc.replaceAll("[0-9]", "");
            }
            for (final MappingSpecificationRow mapSPecs : mapSPecsLists) {
                final String Sourcetablename = mapSPecs.getSourceTableName();
                sourceNamesSet(Sourcetablename.trim(), envMap, mapSPecs, mapName, storprocName, folderName);
                final String targetTableName = mapSPecs.getTargetTableName();
                targetNameSet(targetTableName.trim(), envMap, mapSPecs, mapName, storprocName, folderName);
            }
            final String mapjson = mapper.writeValueAsString((Object)mapObj);
            return mapjson;
        }
        catch (Exception ex) {
            Logger.getLogger(Syncmetadata_Harvard2.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }
    
    public static void sourceNamesSet(final String Sourcetablename, final Map<String, String> envMap, final MappingSpecificationRow mapSPecs, final String mapName, final String storprocName, final String folderName) {
        if (Sourcetablename.split("\n").length > 1) {
            final String[] sourcetablecolumn = Sourcetablename.split("\n");
            envMap.toString();
            final List<String> sourcesystem = getSourceSystem(sourcetablecolumn, mapName, envMap, mapSPecs, storprocName, folderName);
            final List<String> sourceenv = getSourceEnv(sourcetablecolumn, mapName, envMap, mapSPecs, storprocName, folderName);
            final String sourceSystem = StringUtils.join((Iterable)sourcesystem, "\n");
            final String sourceEnv = StringUtils.join((Iterable)sourceenv, "\n");
            mapSPecs.setSourceSystemEnvironmentName(sourceEnv);
            mapSPecs.setSourceSystemName(sourceSystem);
        }
        else if (envMap.containsKey(Sourcetablename)) {
            final String sourceenvSys = envMap.get(Sourcetablename);
            final String SystemName = sourceenvSys.split("#")[1];
            final String environmentName = sourceenvSys.split("#")[0];
            mapSPecs.setSourceSystemName(SystemName);
            mapSPecs.setSourceSystemEnvironmentName(environmentName);
        }
        if (Sourcetablename.toUpperCase().contains("RESULT_OF_") || Sourcetablename.toUpperCase().contains("INSERT-SELECT") || Sourcetablename.toUpperCase().contains("UPDATE-SELECT")) {
            mapSPecs.setSourceTableName("");
            mapSPecs.setSourceTableName(Sourcetablename + "_" + mapName + "_" + folderName);
        }
    }
    
    public static void targetNameSet(final String Targettablename, final Map<String, String> envMap, final MappingSpecificationRow mapSPecs, final String mapName, final String storprocName, final String folderName) {
        if (Targettablename.split("\n").length > 1) {
            final String[] tgttablecolumn = Targettablename.split("\n");
            envMap.toString();
            final List<String> sourcesystem = getSourceSystem(tgttablecolumn, mapName, envMap, mapSPecs, storprocName, folderName);
            final List<String> sourceenv = getSourceEnv(tgttablecolumn, mapName, envMap, mapSPecs, storprocName, folderName);
            final String sourceSystem = StringUtils.join((Iterable)sourcesystem, "\n");
            final String sourceEnv = StringUtils.join((Iterable)sourceenv, "\n");
            mapSPecs.setTargetSystemName(sourceSystem);
            mapSPecs.setTargetSystemEnvironmentName(sourceEnv);
        }
        else if (envMap.containsKey(Targettablename)) {
            final String targetenvSys = envMap.get(Targettablename);
            final String SystemName = targetenvSys.split("#")[1];
            mapSPecs.setTargetSystemName(SystemName);
            final String environmentName = targetenvSys.split("#")[0];
            mapSPecs.setTargetSystemEnvironmentName(environmentName);
        }
        if (Targettablename.toUpperCase().contains("RESULT_OF_") || Targettablename.toUpperCase().contains("INSERT-SELECT") || Targettablename.toUpperCase().contains("UPDATE-SELECT")) {
            mapSPecs.setTargetTableName("");
            mapSPecs.setTargetTableName(Targettablename + "_" + mapName + "_" + folderName);
        }
    }
    
    public static List<String> getSourceSystem(final String[] sourcetablename, final String mapname, final Map<String, String> envMap, final MappingSpecificationRow mapSPecs, final String storprocName, final String folderName) {
        final List<String> sourcesystem = new LinkedList<String>();
        try {
            for (final String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    final String sourceenvSys = envMap.get(sourceTab);
                    sourcesystem.add(sourceenvSys.split("#")[1]);
                }
                else {
                    final String SystemName = mapSPecs.getTargetSystemName();
                    sourcesystem.add(SystemName);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return sourcesystem;
    }
    
    public static List<String> getSourceEnv(final String[] sourcetablename, final String mapname, final Map<String, String> envMap, final MappingSpecificationRow mapSPecs, final String storprocName, final String folderName) {
        final List<String> sourceenv = new LinkedList<String>();
        try {
            for (final String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    final String sourceenvSys = envMap.get(sourceTab);
                    sourceenv.add(sourceenvSys.split("#")[0]);
                }
                else {
                    final String envName = mapSPecs.getTargetSystemEnvironmentName();
                    sourceenv.add(envName);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return sourceenv;
    }
}
