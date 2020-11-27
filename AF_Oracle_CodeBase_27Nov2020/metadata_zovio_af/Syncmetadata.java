package com.erwin.metadata_zovio_af;

import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.sm.SMEnvironment;
import com.ads.api.beans.sm.SMSystem;
import com.ads.api.beans.sm.SMTable;
import com.ads.api.util.SystemManagerUtil;
import static com.erwin.metadata_zovio_af.Syncmetadata.setsrcSysenvNames;
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

/**
 *
 * @author InkolluReddy
 */

public class Syncmetadata {
    public static Mapping metadataSync(Map<String, String> envMap, Mapping mapping, SystemManagerUtil smutill, int projectid, int subjectid, String folderName) {
        String jsonvalue = "";
        try {
            ArrayList<MappingSpecificationRow> mapSPecsLists = mapping.getMappingSpecifications();
            String mapName = mapping.getMappingName();
            String storproc = mapName.substring(0, mapName.lastIndexOf("."));
            String storprocName = storproc.replaceAll("[0-9]", "");
            for (MappingSpecificationRow mapSPecs : mapSPecsLists) {
                String Sourcetablename = mapSPecs.getSourceTableName();
                if(Sourcetablename.equals("RESULT_OF_D"))
                {
                    String a="";
                }
                String br=mapSPecs.getBusinessRule();
                sourceNamesSet(Sourcetablename, envMap, mapSPecs, mapName, storprocName, folderName);
                String Targettablename = mapSPecs.getTargetTableName();
                String[] trgtname = Targettablename.split("\n");
                for (String tgtName : trgtname) {
                    targetNameSet(tgtName, envMap, mapSPecs, mapName, storprocName, folderName);
                }
            }
            changeresultofTarget(mapSPecsLists,mapName);
            changeresultofSource(mapSPecsLists,mapName);
             String query = mapping.getSourceExtractQuery();
            Mapping mapingobj =createMapFromMappingSpecifiactionRow(mapSPecsLists, mapName, query, projectid, subjectid);
            return mapingobj;
        } catch (Exception ex) {
            Logger.getLogger(Syncmetadata.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;

    }

    public static void sourceNamesSet(String Sourcetablename, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName, String folderName) {
        if (Sourcetablename.split("\n").length > 1) {
            String[] sourcetablecolumn = Sourcetablename.split("\n");
            envMap.toString();
            List<String> sourcesystem = getSourceSystem(sourcetablecolumn, mapName, envMap, mapSPecs, storprocName, folderName);
            List<String> sourceenv = getSourceEnv(sourcetablecolumn, mapName, envMap, storprocName, folderName);
            String sourceSystem = StringUtils.join(sourcesystem, "\n");
            String sourceEnv = StringUtils.join(sourceenv, "\n");
            mapSPecs.setSourceSystemEnvironmentName(sourceEnv);
            mapSPecs.setSourceSystemName(sourceSystem);
        } else {
            if (envMap.containsKey(Sourcetablename)) {
                String sourceenvSys = envMap.get(Sourcetablename);
                if (setsrcSysenvNames(Sourcetablename, mapName) == 0) {
                    String SystemName = sourceenvSys.split("#")[1];
                    String environmentName = sourceenvSys.split("#")[0];
                    mapSPecs.setSourceSystemName(SystemName);
                    mapSPecs.setSourceSystemEnvironmentName(environmentName);
                } else {
                    mapSPecs.setSourceSystemEnvironmentName(folderName + "_" + storprocName);
                    mapSPecs.setSourceSystemName(folderName + "_" + storprocName);
                }
            } else {
                mapSPecs.setSourceSystemEnvironmentName(folderName + "_" + storprocName);
                mapSPecs.setSourceSystemName(folderName + "_" + storprocName);
            }
            if (Sourcetablename.toUpperCase().contains("RESULT_OF_") || Sourcetablename.toUpperCase().contains("INSERT-SELECT")||Sourcetablename.toUpperCase().contains("UPDATE-SELECT")) {
                mapSPecs.setSourceTableName(Sourcetablename + "_" + mapName);
            }

        }
    }

    public static void targetNameSet(String Targettablename, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName, String folderName) {

        if (envMap.containsKey(Targettablename.toUpperCase().trim())) {
            String targetenvSys = envMap.get(Targettablename);
            String SystemName = targetenvSys.split("#")[1];
            mapSPecs.setTargetSystemName(SystemName);
            String environmentName = targetenvSys.split("#")[0];
            mapSPecs.setTargetSystemEnvironmentName(environmentName);
        } else {
            mapSPecs.setTargetSystemName(folderName + "_" + storprocName);
            mapSPecs.setTargetSystemEnvironmentName(folderName + "_" + storprocName);
        }
        if (Targettablename.toUpperCase().contains("RESULT_OF_") || Targettablename.toUpperCase().contains("INSERT-SELECT")||Targettablename.toUpperCase().contains("UPDATE-SELECT")) {
            mapSPecs.setTargetTableName(Targettablename + "_" + mapName);
        }

    }

    public static Map<String, String> metaDatacreation(SystemManagerUtil smutill) {
        Map<String, String> metaDatacreationmap = new HashMap();
        try {
            ArrayList<SMSystem> systems = smutill.getSystems();
            for (int i = 0; i < systems.size(); i++) {
                String sysName = systems.get(i).getSystemName();
                int sysid = systems.get(i).getSystemId();
                ArrayList<SMEnvironment> environments = smutill.getEnvironments(sysid);
                for (int j = 0; j < environments.size(); j++) {
                    int envid = environments.get(j).getEnvironmentId();
                    SMEnvironment environment = smutill.getEnvironment(envid);
                    String envName = environment.getSystemEnvironmentName();
                    SMEnvironment smEnv = smutill.getEnvironment(envid, true);
                    List<SMTable> envtables = smutill.getEnvironmentTables(envid);
                    for (int k = 0; k < envtables.size(); k++) {
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
        mapping.setMappingSpecifications(specrowlist);
        mapping.setSourceExtractQuery(query);
        mapping.setProjectId(projectid);
        mapping.setSubjectId(subjectid);
        return mapping;
    }

    public static List<String> getSourceSystem(String[] sourcetablename, String mapname, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String storprocName, String folderName) {
        List<String> sourcesystem = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);
                    if (setsrcSysenvNames(sourceTab, mapname) == 0) {
                        sourcesystem.add(sourceenvSys.split("#")[1]);
                    } else {
                        sourcesystem.add(folderName + "_" + storprocName);
                    }
                } else {
                    sourcesystem.add(folderName + "_" + storprocName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourcesystem;
    }

    public static List<String> getSourceEnv(String[] sourcetablename, String mapname, Map<String, String> envMap, String storprocName, String folderName) {
        List<String> sourceenv = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);
                    if (setsrcSysenvNames(sourceTab, mapname) == 0) {
                        sourceenv.add(sourceenvSys.split("#")[0]);
                    } else {
                        sourceenv.add(folderName + "_" + storprocName);
                    }
                } else {
                    sourceenv.add(folderName + "_" + storprocName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceenv;
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
