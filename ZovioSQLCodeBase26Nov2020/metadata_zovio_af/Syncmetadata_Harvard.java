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
public class Syncmetadata_Harvard {
     public static String metadataSync(Map<String, String> envMap, SystemManagerUtil smutill, String json,String folderName) {
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
            String storprocName =storproc.replaceAll("[0-9]", "");
            if(mapName.contains(".")){
            storproc = mapName.substring(0, mapName.lastIndexOf("."));
            storprocName = storproc.replaceAll("[0-9]", "");  
            }
            for (MappingSpecificationRow mapSPecs : mapSPecsLists) {
                String Sourcetablename = mapSPecs.getSourceTableName();
              
                sourceNamesSet(Sourcetablename.trim().toUpperCase(), envMap, mapSPecs, mapName, storprocName,folderName);
                String targetTableName = mapSPecs.getTargetTableName();
              
                    targetNameSet(targetTableName.trim().toUpperCase(), envMap, mapSPecs, mapName, storprocName,folderName);
            }
           
            String mapjson = mapper.writeValueAsString((Object) mapObj);
            return mapjson;
        } catch (Exception ex) {
            Logger.getLogger(Syncmetadata_Natixis.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }
     public static void sourceNamesSet(String Sourcetablename, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName, String folderName) {
        if (Sourcetablename.split("\n").length > 1) {
            String[] sourcetablecolumn = Sourcetablename.split("\n");
            envMap.toString();
            List<String> sourcesystem = getSourceSystem(sourcetablecolumn, mapName, envMap, mapSPecs, storprocName, folderName);
            List<String> sourceenv = getSourceEnv(sourcetablecolumn, mapName, envMap,mapSPecs,storprocName, folderName);
            String sourceSystem= StringUtils.join(sourcesystem,"\n");
            String sourceEnv=StringUtils.join(sourceenv,"\n");
            
            mapSPecs.setSourceSystemEnvironmentName(sourceEnv);
            mapSPecs.setSourceSystemName(sourceSystem);
        }
        
          else if (envMap.containsKey(Sourcetablename)) {
                String sourceenvSys = envMap.get(Sourcetablename);
                
                    String SystemName = sourceenvSys.split("#")[1];
                    String environmentName = sourceenvSys.split("#")[0];
                    mapSPecs.setSourceSystemName(SystemName);
                    mapSPecs.setSourceSystemEnvironmentName(environmentName);
            } 
            if (Sourcetablename.toUpperCase().contains("RESULT_OF_") || Sourcetablename.toUpperCase().contains("INSERT-SELECT")||Sourcetablename.toUpperCase().contains("UPDATE-SELECT")) {
                mapSPecs.setSourceTableName("");
                mapSPecs.setSourceTableName(Sourcetablename + "_" + mapName+"_"+folderName);
            }
    }
     public static void targetNameSet(String Targettablename, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName, String folderName) {
         if (Targettablename.split("\n").length > 1) {
            String[] tgttablecolumn = Targettablename.split("\n");
            envMap.toString();
            List<String> sourcesystem = getSourceSystem(tgttablecolumn, mapName, envMap, mapSPecs, storprocName, folderName);
            List<String> sourceenv = getSourceEnv(tgttablecolumn, mapName, envMap,mapSPecs,storprocName, folderName);
            String sourceSystem= StringUtils.join(sourcesystem,"\n");
            String sourceEnv=StringUtils.join(sourceenv,"\n");
            
            mapSPecs.setTargetSystemName(sourceSystem);
            mapSPecs.setTargetSystemEnvironmentName(sourceEnv);
        }
         else if (envMap.containsKey(Targettablename)) {
            String targetenvSys = envMap.get(Targettablename);
            String SystemName = targetenvSys.split("#")[1];
            mapSPecs.setTargetSystemName(SystemName);
            String environmentName = targetenvSys.split("#")[0];
            mapSPecs.setTargetSystemEnvironmentName(environmentName);
        } 
        if (Targettablename.toUpperCase().contains("RESULT_OF_") || Targettablename.toUpperCase().contains("INSERT-SELECT")||Targettablename.toUpperCase().contains("UPDATE-SELECT")) {
            mapSPecs.setTargetTableName("");
            mapSPecs.setTargetTableName(Targettablename +"_"+mapName+"_"+folderName);
        }

    }
      
       public static List<String> getSourceSystem(String[] sourcetablename, String mapname, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String storprocName, String folderName) {
        List<String> sourcesystem = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);
                   
                        sourcesystem.add(sourceenvSys.split("#")[1]);
                    } 
                 else {
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
        public static List<String> getSourceEnv(String[] sourcetablename, String mapname, Map<String, String> envMap,MappingSpecificationRow mapSPecs, String storprocName, String folderName) {
        List<String> sourceenv = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);
                    
                        sourceenv.add(sourceenvSys.split("#")[0]);
                }  else {
                      String envName = mapSPecs.getTargetSystemEnvironmentName();
                        sourceenv.add(envName);
                    }
                } 
            }
         catch (Exception e) {
            e.printStackTrace();
        }
        return sourceenv;
    }
    public static void changeresplitTable(ArrayList<MappingSpecificationRow> mapspeclist) {
        try{
        Set<String> targettabset = new LinkedHashSet();
        Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
        while (iter.hasNext()) {
            MappingSpecificationRow row = iter.next();
            String targetTabName = row.getTargetTableName();
            String SourceTableName = row.getSourceTableName();
         String[] Sourcetablenamearray = SourceTableName.split("\n");
                String Srctablename="";
                String TgtTabName="";
         for (String Sourcetablename : Sourcetablenamearray) { 
        String stab[]=Sourcetablename.split("\\.");
            if (stab.length>=2) {
                Srctablename = stab[stab.length-2]+"."+ stab[stab.length-1];
            }
         }
            String[]  targetTabNamearray=targetTabName.split("\n");
           for(String TargetTabName : targetTabNamearray) {
               String ttab[]=TargetTabName.split("\\.");
                if (ttab.length>=2) {
                TgtTabName = ttab[ttab.length-2]+"."+ ttab[ttab.length-1];
            }
           }
           if(!Srctablename.isEmpty())
           {
           row.setSourceTableName(Srctablename);
           }
           if(!TgtTabName.isEmpty())
           {
           row.setTargetTableName(TgtTabName);
           }
        }
    }
    catch(Exception e)
    {
         e.printStackTrace();
        Logger.getLogger(Syncmetadata_Natixis.class.getName()).log(Level.SEVERE, null, e);
    }
    }
    public static void changeresultofTarget(ArrayList<MappingSpecificationRow> mapspeclist) {
        try{
        Set<String> targettabset = new LinkedHashSet();
        Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
        while (iter.hasNext()) {
            MappingSpecificationRow row = iter.next();
            StringBuilder sourcetabsb=new StringBuilder();
            StringBuilder targettabsb=new StringBuilder();
            String targetTabName = row.getTargetTableName().replace("[", "").replace("]", "");
            String SourceTableName = row.getSourceTableName().replace("[", "").replace("]", "");
            String targetColumnName = row.getTargetColumnName().replace("[", "").replace("]", "");
            String sourceColumnName = row.getSourceColumnName().replace("[", "").replace("]", "");
            String[] Sourcetablenamearray = SourceTableName.split("\n");
            for (String Sourcetablename : Sourcetablenamearray) {
            if (!Sourcetablename.toUpperCase().contains("RESULT") && !Sourcetablename.toUpperCase().contains("INSERT") && !Sourcetablename.toUpperCase().contains("UPDATE")) {
                if (!Sourcetablename.contains(".") && !"".equals(Sourcetablename)) {
                    Sourcetablename = "dbo." + Sourcetablename;
                    sourcetabsb.append(Sourcetablename).append("\n");
                }
            }
            }
            String[]  targetTabNamearray=targetTabName.split("\n");
           for(String TargetTabName : targetTabNamearray) { 
            if (!TargetTabName.toUpperCase().contains("RESULT") && !TargetTabName.toUpperCase().contains("INSERT") && !TargetTabName.toUpperCase().contains("UPDATE")) {
                if (!TargetTabName.contains(".") && !"".equals(TargetTabName)) {
                 TargetTabName = "dbo." + TargetTabName;
                    targettabsb.append(TargetTabName).append("\n");
                }
            }
           }
           if(!sourcetabsb.toString().isEmpty())
           {
            row.setSourceTableName(sourcetabsb.toString().trim());
           }
           else
           {
             row.setSourceTableName(SourceTableName);  
           }
           if(!targettabsb.toString().isEmpty())
           {
            row.setTargetTableName(targettabsb.toString().trim());
           }
           else
           {
              row.setTargetTableName(targetTabName); 
           }
            row.setSourceColumnName(sourceColumnName);
            row.setTargetColumnName(targetColumnName);
        }
    }
        catch(Exception e)
        {
            e.printStackTrace();
            Logger.getLogger(Syncmetadata_Natixis.class.getName()).log(Level.SEVERE, null, e);
        }
    }
}
