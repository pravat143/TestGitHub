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
import static com.erwin.metadata.SyncmetadataPWCV1.sourceNamesSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class SyncmetadataPWC {
 static  StringBuilder sysenvtabsb = new StringBuilder();
    public static String metadataSync(Map<String, String> envMap, SystemManagerUtil smutill, String json) {
        String jsonvalue = "";
        String Sourcetablename="";
        try {
            ObjectMapper mapper = new ObjectMapper();
            json = json.replace(",\"childNodes\":[]","");
            List<Mapping> mapObj = (List<Mapping>) mapper.readValue(json, (TypeReference) new TypeReference<List<Mapping>>() {
            });
            ArrayList<MappingSpecificationRow> mapSPecsLists = (ArrayList<MappingSpecificationRow>) mapObj.get(0).getMappingSpecifications();
            envMap.toString();
            String mapName = mapObj.get(0).getMappingName();
            String databaseName = mapName.split("@@@")[0];
            String storproc=  mapName.split("@@@")[1];
            String storprocName = storproc.replaceAll("[0-9]", "");
        //    if (mapName.contains(".")) {
        //        storproc = mapName.substring(0, mapName.lastIndexOf("."));
        //        storprocName = storproc.replaceAll("[0-9]", "");
        //    }
            changeresultofTarget(mapSPecsLists);
            for (MappingSpecificationRow mapSPecs : mapSPecsLists) {
                envMap.toString();
        //SourceSide 
                String Sourcetablenames = mapSPecs.getSourceTableName();
                String[] Sourcetablenamearray = Sourcetablenames.split("\n");
                
                StringBuilder srcsystemsb = new StringBuilder();
                StringBuilder srcenvsb = new StringBuilder();
                StringBuilder srcTabsb =new StringBuilder();
                for (String SourcetableNames : Sourcetablenamearray) {
                 if (SourcetableNames.split("\\.").length==3) {
                        Sourcetablenames = SourcetableNames.split("\\.")[0];
                     }
                      else
                      {
                         if (SourcetableNames.toUpperCase().contains("RESULT_OF_") || Sourcetablename.toUpperCase().contains("INSERT-SELECT")||Sourcetablename.toUpperCase().contains("UPDATE-SELECT")) {
                             mapSPecs.setSourceTableName(SourcetableNames + "_" +databaseName+"_" + storproc);
                            }
                         else 
                         {
                             Sourcetablenames=databaseName;
                         }
                      }
               //  sourceNamesSet(Sourcetablename, envMap, mapSPecs, mapName, storprocName);
                 String SystemName = gettargetSystemNameSet(Sourcetablenames.trim(), envMap, mapName, storprocName);
                 if ("".equals(SystemName)) {
                        SystemName = mapSPecs.getTargetSystemName();
                    }
                 srcsystemsb.append(SystemName).append("\n");
                 String EnvironmentName = gettargetEnvironmentNameSet(Sourcetablenames.trim(), envMap, mapName, storprocName);
                    if ("".equals(EnvironmentName)) {
                        EnvironmentName = mapSPecs.getTargetSystemEnvironmentName();
                    }
                    srcenvsb.append(EnvironmentName).append("\n");
                }
                mapSPecs.setSourceSystemEnvironmentName(srcenvsb.toString().trim());
                mapSPecs.setSourceSystemName(srcsystemsb.toString().trim());
             //   System.out.println("96......."+srcTabsb.toString().trim());
           //Targetside
                String targetTableName = mapSPecs.getTargetTableName();
                String[] targetval = targetTableName.split("\n");
                StringBuilder Tgtsystemsb = new StringBuilder();
                StringBuilder Tgtenvsb = new StringBuilder();
                for (String tgtTabName : targetval) {
                      if (tgtTabName.split("\\.").length==3) {
                        tgtTabName = tgtTabName.split("\\.")[0];
                     }
                      else
                      {
                         if (tgtTabName.toUpperCase().contains("RESULT_OF_") ||tgtTabName.toUpperCase().contains("INSERT-SELECT")|| tgtTabName.toUpperCase().contains("UPDATE-SELECT")) 
                            {
                                mapSPecs.setTargetTableName(tgtTabName+"_"+databaseName+"_" + storproc);
                            }
                         else 
                         {
                             tgtTabName=databaseName;
                         }
                      }
                    String SystemName = gettargetSystemNameSet(tgtTabName.trim(), envMap, mapName, storprocName);
                    sysenvtabsb.append("tableName"+tgtTabName).append("\n");
                    if ("".equals(SystemName)) {
                        SystemName = mapSPecs.getTargetSystemName();
                    }
                    sysenvtabsb.append("SystemName"+SystemName).append("\n");
                    Tgtsystemsb.append(SystemName).append("\n");
                    String EnvironmentName = gettargetEnvironmentNameSet(tgtTabName.trim(), envMap, mapName, storprocName);
                    if ("".equals(EnvironmentName)) {
                        EnvironmentName = mapSPecs.getTargetSystemEnvironmentName();
                    }
                    sysenvtabsb.append("EnvName"+EnvironmentName).append("\n");
                    Tgtenvsb.append(EnvironmentName).append("\n");
                }
                mapSPecs.setTargetSystemEnvironmentName(Tgtenvsb.toString().trim());
                mapSPecs.setTargetSystemName(Tgtsystemsb.toString().trim());
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
         //   System.out.println("sourceTab---"+sourceTab);
          //  mapSPecs.setSourceTableName(sourceTab);
          //  envMap.toString();
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
    //            System.out.println("line no 172"+schemaname + "." + Sourcetablename);
                mapSPecs.setSourceTableName(schemaname + "." + Sourcetablename);
            }
            mapSPecs.setSourceSystemName(SystemName);
            mapSPecs.setSourceSystemEnvironmentName(environmentName);
        } else {
            String envName = "";
            if (Sourcetablename.split("\\.").length == 3) {
                envName = Sourcetablename.split("\\.")[0];
                if (envMap.containsKey(envName.toUpperCase())) {
                    String envstr = envMap.get(envName.toUpperCase());
                    String SystemName = envstr.split("#")[1];
                    String environmentName = envstr.split("#")[0];
                    mapSPecs.setSourceSystemEnvironmentName(environmentName);
                    mapSPecs.setSourceSystemName(SystemName);
                }
            }
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static String gettargetSystemNameSet(String targetTableName, Map<String, String> envMap, String mapName, String storprocName) {
        String systemName = "";
        try {
            if (envMap.containsKey(targetTableName.toUpperCase())) {
                String targetenvSys = envMap.get(targetTableName.toUpperCase());
                systemName = targetenvSys.split("#")[1];
            }
            if (targetTableName.split("\\.").length == 3) {
                systemName = targetTableName.split("\\.")[1];
                if (envMap.containsKey(systemName.toUpperCase())) {
                    String envstr = envMap.get(systemName.toUpperCase());
                    systemName = envstr.split("#")[1];
                }
            } 
        } catch (Exception e) {
            e.printStackTrace();
        }
        return systemName;
    }
     public static String getSourceSystemNameSet(String targetTableName, Map<String, String> envMap, String mapName, String storprocName) {
        String systemName = "";
        try {
            if (envMap.containsKey(targetTableName.toUpperCase())) {
                String targetenvSys = envMap.get(targetTableName.toUpperCase());
                systemName = targetenvSys.split("#")[1];
            }
            if (targetTableName.split("\\.").length == 3) {
                systemName = targetTableName.split("\\.")[1];
                if (envMap.containsKey(systemName.toUpperCase())) {
                    String envstr = envMap.get(systemName.toUpperCase());
                    systemName = envstr.split("#")[1];
                }
            } 
        } catch (Exception e) {
            e.printStackTrace();
        }
        return systemName;
    }
   public static String gettargetEnvironmentNameSet(String targetTableName, Map<String, String> envMap, String mapName, String storprocName) {
        String environmentName = "";
        try {
            if (envMap.containsKey(targetTableName.toUpperCase())) {
                String targetenvSys = envMap.get(targetTableName.toUpperCase());
                environmentName = targetenvSys.split("#")[0];
            }
            if (targetTableName.split("\\.").length == 3) {
                environmentName = targetTableName.split("\\.")[0];
                if (envMap.containsKey(environmentName.toUpperCase())) {
                    String envstr = envMap.get(environmentName.toUpperCase());
                    environmentName = envstr.split("#")[1];
                }
            } 
        } catch (Exception e) {
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
    public static void changeresplitTable(ArrayList<MappingSpecificationRow> mapspeclist) {
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
           
            if (Sourcetablename.split("\\.").length == 3) {
                Srctablename = Sourcetablename.split("\\.")[1]+"."+ Sourcetablename.split("\\.")[2];
            }
         }
            String[]  targetTabNamearray=targetTabName.split("\n");
           for(String TargetTabName : targetTabNamearray) { 
                if (TargetTabName.split("\\.").length == 3) {
                    TgtTabName = TargetTabName.split("\\.")[1] + "." + TargetTabName.split("\\.")[2];
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
}
