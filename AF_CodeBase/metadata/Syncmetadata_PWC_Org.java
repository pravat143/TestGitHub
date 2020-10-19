// 
// Decompiled by Procyon v0.5.36
// 
package com.erwin.metadata;

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
import static com.erwin.metadata.Syncmetadata.changeresultofSource;
import static com.erwin.metadata.Syncmetadata.changeresultofTarget;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class Syncmetadata_PWC_Org {

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
            String storprocName =storproc.replaceAll("[0-9]", "");
            if(mapName.contains(".")){
            storproc = mapName.substring(0, mapName.lastIndexOf("."));
            storprocName = storproc.replaceAll("[0-9]", "");
            }
            
            for (MappingSpecificationRow mapSPecs : mapSPecsLists) {
              envMap.toString();
              
                String Sourcetablename = mapSPecs.getSourceTableName();
                sourceNamesSet(Sourcetablename.trim(), envMap, mapSPecs, mapName, storprocName);
                String targetTableName = mapSPecs.getTargetTableName();
                String[] split;
                String[] trgtname = split = targetTableName.split("\n");
                StringBuilder systemsb = new StringBuilder();
                StringBuilder envsb = new StringBuilder();
                for (String tgtName : split) {
                  //  if (tgtName.contains(".")) {
                    //    tgtName = tgtName.split("\\.")[1];
                   // }
                   // targetNameSet(tgtName, envMap, mapSPecs, mapName, storprocName);
                    String SystemName = gettargetSystemNameSet(tgtName, envMap, mapName, storprocName);
                    systemsb.append(SystemName).append("\n");
                    String EnvironmentName = gettargetEnvironmentNameSet(tgtName, envMap, mapName, storprocName);
                    envsb.append(EnvironmentName).append("\n");
                    
                    
                }  
                mapSPecs.setTargetSystemEnvironmentName(envsb.toString().trim());
                mapSPecs.setTargetSystemName(systemsb.toString().trim());
                
            }
             changeresultofTarget(mapSPecsLists,mapName);
            changeresultofSource(mapSPecsLists,mapName);
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
        if (Sourcetablename.toUpperCase().contains("RESULT_OF_") || Sourcetablename.toUpperCase().contains("INSERT")||Sourcetablename.toUpperCase().contains("UPDATE")||Sourcetablename.toUpperCase().contains("MERGE")) {
            mapSPecs.setTargetTableName(Sourcetablename + "_" + mapName);
        }
        else if (envMap.containsKey(Sourcetablename.toUpperCase())) {
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

    public static void targetNameSet(String targetTableName, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName) {
        
        try{
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
        }else{
            mapSPecs.setTargetTableName(targetTableName);
        }
         if (targetTableName.toUpperCase().contains("RESULT_OF_") || targetTableName.toUpperCase().contains("INSERT-SELECT")||targetTableName.toUpperCase().contains("UPDATE-SELECT")) {
            mapSPecs.setTargetTableName(targetTableName + "_" + mapName);
        }
        }catch(Exception e){
        e.printStackTrace();
        }
     
         
         
    }
    public static String gettargetSystemNameSet(String targetTableName, Map<String, String> envMap, String mapName, String storprocName) {
       String SystemName ="";
        try{
           if (envMap.containsKey(targetTableName.toUpperCase())) {
            String targetenvSys = envMap.get(targetTableName.toUpperCase());
             SystemName = targetenvSys.split("#")[1];
          
          
        }else{
          
        }
         if (targetTableName.toUpperCase().contains("RESULT_OF_") || targetTableName.toUpperCase().contains("INSERT-SELECT")||targetTableName.toUpperCase().contains("UPDATE-SELECT")) {
         SystemName=  targetTableName + "_" + mapName;
        }
        }catch(Exception e){
        e.printStackTrace();
        }
     
         return SystemName;
         
    }
     public static String gettargetEnvironmentNameSet(String targetTableName, Map<String, String> envMap, String mapName, String storprocName) {
       String environmentName ="";
        try{
           if (envMap.containsKey(targetTableName.toUpperCase())) {
            String targetenvSys = envMap.get(targetTableName.toUpperCase());
             environmentName = targetenvSys.split("#")[0];
          
          
        }else{
          
        }
         if (targetTableName.toUpperCase().contains("RESULT_OF_") || targetTableName.toUpperCase().contains("INSERT-SELECT")||targetTableName.toUpperCase().contains("UPDATE-SELECT")) {
         environmentName=  targetTableName + "_" + mapName;
        }
        }catch(Exception e){
        e.printStackTrace();
        }
     
         return environmentName;
         
    }

    public static Map<String, String> metaDatacreation(SystemManagerUtil smutill) {
        Map<String, String> metaDatacreationmap = new HashMap<String, String>();
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
                        String envsys = envName + "#" + sysName;
                        metaDatacreationmap.put(tableName.toUpperCase(), envsys);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return metaDatacreationmap;
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
                } else {
                    sourcesystem.add(mapSPecs.getSourceSystemName());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourcesystem;
    }

    public static List<String> getSourceEnv(String[] sourcetablename, String mapname, Map<String, String> envMap, MappingSpecificationRow mapSPecs,String storprocName) {
        List<String> sourceenv = new LinkedList<String>();
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);
                    sourceenv.add(sourceenvSys.split("#")[0]);
                } else {
                    sourceenv.add(mapSPecs.getSourceSystemEnvironmentName());
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
      public static void changeresultofTarget(ArrayList<MappingSpecificationRow> mapspeclist,String mapname) {
         Set<String> targettabset = new LinkedHashSet();
        Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
       
        while (iter.hasNext()) {
            MappingSpecificationRow row = iter.next();
            String targetTabName = row.getTargetTableName();
            if(!targetTabName.toUpperCase().contains(mapname.toUpperCase()))
            {
            String targetarr[] = targetTabName.split("\n");
            for (String targettab : targetarr) {
                if(targettab.toUpperCase().contains("RESULT_OF")){
                
                targettabset.add(targettab.toUpperCase()+"_"+mapname);
                }
            }
            if(!targettabset.isEmpty())
            {
            String appendtargetTabName = StringUtils.join(targettabset,"\n");
            System.out.println(appendtargetTabName);
            row.setTargetTableName(appendtargetTabName);
            }
            targettabset.clear();
        }
        }
    }
     public static void changeresultofSource(ArrayList<MappingSpecificationRow> mapspeclist,String mapname) {
         Set<String> sourcetabset = new LinkedHashSet();
        Iterator<MappingSpecificationRow> iter = mapspeclist.iterator();
       
        while (iter.hasNext()) {
            MappingSpecificationRow row = iter.next();
            String sourceTabName = row.getSourceTableName();
            if(!sourceTabName.toUpperCase().contains(mapname.toUpperCase()))
            {
            String sourcearr[] = sourceTabName.split("\n");
            for (String sourcetab : sourcearr) {
                if(sourcetab.toUpperCase().contains("RESULT_OF")){
                
                sourcetabset.add(sourcetab.toUpperCase()+"_"+mapname);
                }
            }
            if(!sourcetabset.isEmpty())
            {
            String appendsource = StringUtils.join(sourcetabset,"\n");
            System.out.println(appendsource);
            row.setSourceTableName(appendsource);
            }
            sourcetabset.clear();
        }
    }

     }
}
