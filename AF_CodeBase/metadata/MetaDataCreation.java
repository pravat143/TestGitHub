/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.metadata;

import com.ads.api.beans.sm.SMEnvironment;
import com.ads.api.beans.sm.SMSystem;
import com.ads.api.beans.sm.SMTable;
import com.ads.api.util.SystemManagerUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author InkolluReddy
 */
public class MetaDataCreation {
    
    public static Map<String, String> metaDatacreation(SystemManagerUtil smutill,String SystemName) {
        Map<String, String> metaDatacreationmap = new HashMap();
        try {
            ArrayList<SMSystem> systems = smutill.getSystems();
            for (int i = 0; i < systems.size(); i++) {
                String sysName = systems.get(i).getSystemName();
                   
                 if(sysName.equalsIgnoreCase(SystemName)) 
                 {
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
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return metaDatacreationmap;
    }
    
}
