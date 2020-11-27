/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.metadata_zovio_af;

import com.ads.api.beans.mm.MappingSpecificationRow;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 *
 * @author InkolluReddy
 */
public class ExtreamSourceAndExtreamTarget {

    public static Set<String> getSourcetargetMap(ArrayList<MappingSpecificationRow> speclist) {
        Set<String> set = new HashSet<>();
        Set<String> intermediateSet = new HashSet<>();
        Set<String> bothSet = new HashSet<>();
        Set<String> sourceTableSet = new HashSet<>();
        Set<String> targetTableSet = new HashSet<>();

        for (MappingSpecificationRow mappingSpecificationRow : speclist) {
            String sourcetableName = mappingSpecificationRow.getSourceTableName() + "##" + mappingSpecificationRow.getSourceSystemName() + "##" + mappingSpecificationRow.getSourceSystemEnvironmentName();
            String targetTableName = mappingSpecificationRow.getTargetTableName() + "##" + mappingSpecificationRow.getTargetSystemName() + "##" + mappingSpecificationRow.getTargetSystemEnvironmentName();
            set.add(sourcetableName.toUpperCase());
            set.add(targetTableName.toUpperCase());
            sourceTableSet.add(sourcetableName.toUpperCase());
            targetTableSet.add(targetTableName.toUpperCase());
        }
        Set<String> extremeSourceSet = getExtremeSource(sourceTableSet, targetTableSet);
        Set<String> extremeTargetSet = getExtremeTarget(sourceTableSet, targetTableSet);
        Iterator value = set.iterator();
        bothSet.addAll(extremeSourceSet);
        bothSet.addAll(extremeTargetSet);
        while (value.hasNext()) {
            String value1 = value.next().toString();
            if (!bothSet.contains(value1)) {
                if (value1.contains("##")) {
                    String tableName = value1.split("\\#\\#")[0];
                    if (!tableName.contains(".")) {
                        intermediateSet.add(value1.split("\\#\\#")[0]);
                    }
                } else {
                    intermediateSet.add(value1);
                }
            }
        }
        return intermediateSet;
    }

    public static Set<String> getExtremeTargetSet(ArrayList<MappingSpecificationRow> speclist) {
        Set<String> newtargetTableSet = new HashSet<>();
        try {
            Set<String> set = new HashSet<>();
            //Set<String> intermediateSet = new HashSet<>();
            //Set<String> bothSet = new HashSet<>();
            Set<String> sourceTableSet = new LinkedHashSet<>();
            Set<String> targetTableSet = new LinkedHashSet<>();
            for (MappingSpecificationRow mappingSpecificationRow : speclist) {
                String sourcetableName = mappingSpecificationRow.getSourceTableName() + "##" + mappingSpecificationRow.getSourceSystemName() + "##" + mappingSpecificationRow.getSourceSystemEnvironmentName();
                String targetTableName = mappingSpecificationRow.getTargetTableName() + "##" + mappingSpecificationRow.getTargetSystemName() + "##" + mappingSpecificationRow.getTargetSystemEnvironmentName();
                set.add(sourcetableName.toUpperCase());
                set.add(targetTableName.toUpperCase());
                sourceTableSet.add(sourcetableName.toUpperCase());
                targetTableSet.add(targetTableName.toUpperCase());
            }
            // Set<String> extremeSourceSet = getExtremeSource(sourceTableSet, targetTableSet);
            Set<String> extremeTargetSet = getExtremeTarget(sourceTableSet, targetTableSet);
            Iterator value = extremeTargetSet.iterator();
//        bothSet.addAll(extremeSourceSet);
//        bothSet.addAll(extremeTargetSet);
            while (value.hasNext()) {
                String value1 = value.next().toString();
                //if (!bothSet.contains(value1)) {
                if (value1.contains("##")) {

                    String tableName[] = value1.split("\\#\\#");
                    //if (!tableName.contains(".")) {
                    if (tableName.length > 0) {
                        newtargetTableSet.add(value1.split("\\#\\#")[0]);
                    }
                    //}
                } else {
                    newtargetTableSet.add(value1);
                }
                // }

            }

        } catch (Exception e) {
            e.printStackTrace();

        }
        return newtargetTableSet;
    }

    public static Set<String> getExtremesrcSet(ArrayList<MappingSpecificationRow> speclist) {
        Set<String> set = new HashSet<>();
        //Set<String> intermediateSet = new HashSet<>();
        //Set<String> bothSet = new HashSet<>();
        Set<String> sourceTableSet = new HashSet<>();
        Set<String> targetTableSet = new HashSet<>();

        for (MappingSpecificationRow mappingSpecificationRow : speclist) {
            String sourcetableName = mappingSpecificationRow.getSourceTableName();
            String targetTableName = mappingSpecificationRow.getTargetTableName();
            set.add(sourcetableName.toUpperCase());
            set.add(targetTableName.toUpperCase());
            sourceTableSet.add(sourcetableName.toUpperCase());
            targetTableSet.add(targetTableName.toUpperCase());
        }
        Set<String> extremeSourceSet = getExtremeSource(sourceTableSet, targetTableSet);
        //    Set<String> extremeTargetSet = getExtremeTarget(sourceTableSet, targetTableSet);
//        Iterator value = set.iterator();
//        bothSet.addAll(extremeSourceSet);
//        bothSet.addAll(extremeTargetSet);
//        while (value.hasNext()) {
//            String value1 = value.next().toString();
//            if (!bothSet.contains(value1)) {
//                intermediateSet.add(value1);
//            }
//        }
        return extremeSourceSet;
    }

    public static Set<String> getExtremeSource(Set<String> sourceTableSet, Set<String> targetTableSet) {
        Set<String> extremeSourceSet = new HashSet<>();
        try {
            Iterator value = sourceTableSet.iterator();
            while (value.hasNext()) {
                String value1 = value.next().toString();
                if (!targetTableSet.contains(value1)) {
                    extremeSourceSet.add(value1);
                }
            }

        } catch (Exception e) {
        }
        return extremeSourceSet;
    }

    public static Set<String> getExtremeTarget(Set<String> sourceTableSet, Set<String> targetTableSet) {
        Set<String> extremeTargetSet = new LinkedHashSet<>();
        try {
            Iterator value = targetTableSet.iterator();
            while (value.hasNext()) {
                String value1 = value.next().toString();
                if (!sourceTableSet.contains(value1)) {
                    extremeTargetSet.add(value1);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return extremeTargetSet;
    }

}
