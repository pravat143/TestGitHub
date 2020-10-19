/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.sqlparser.client_AF;

import com.ads.api.beans.mm.MappingSpecificationRow;
import com.erwin.dataflow.model.xml.dataflow;
import com.erwin.dataflow.model.xml.relation;
import com.erwin.dataflow.model.xml.sourceColumn;
import com.erwin.dataflow.model.xml.table;
import com.erwin.dataflow.model.xml.targetColumn;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Balaji
 */
public class RelationAnalyzer_AF {

    public static final String DELIMITER = "#@ERWIN@#";
    private LinkedHashMap<String, String> tableAliasMap;
    private LinkedHashMap<String, String> businessRuleMap;
    private LinkedHashMap<String, HashSet<String>> keyValuesMap;
    private LinkedHashMap<String, String> joinComponentMap;
    private LinkedHashMap<String, String> tableSchemaMap;
     
    public ArrayList<MappingSpecificationRow> analyzeRelations(dataflow dtflow, String[] sysEnvDetails) {
        tableAliasMap = new LinkedHashMap<>();
        businessRuleMap = new LinkedHashMap<>();
        keyValuesMap = new LinkedHashMap<>();
        joinComponentMap = new LinkedHashMap<>();
        tableSchemaMap=new LinkedHashMap<>();
        List<table> tables = dtflow.getTables();
        //removeDupilcatesWithAliasName(tables);
        
        List<table> resultsets = dtflow.getResultsets();
        List<relation> relations = dtflow.getRelations();
        if (relations == null) {
            return new ArrayList<>();
        }
        prepareAliasMap(tables);
        prepareAliasMap(resultsets);
        prepareSchemaMap(tables);
        
        LinkedHashMap<String, HashSet<String>> srcTgtCmpnt = new LinkedHashMap<>();
        for (relation relation : relations) {
            prepareBusinessRules(relation);
            prepareKeyValues(relation);
            prepareSrcTgtMap(relation, srcTgtCmpnt);
        }

        preparejoinComponent(srcTgtCmpnt);
        businessRuleMap = ModifyBusinessRuleMap();

        ArrayList<MappingSpecificationRow> specRows = new ArrayList<>();
        for (relation relation : relations) {
            if (!relation.getType().equals("lineage") || relation.getTarget() == null
                    || relation.getSources() == null) {
                continue;
            }
            List<sourceColumn> sources = relation.getSources();
            targetColumn target = relation.getTarget();
            String tgtTableName = target.getParent_name();
            String tgtColumnName = target.getColumn();
            String srcTableName = "";
            String srcColumnName = "";
            String srcTableName1="";
            HashSet<String> duplicateSources = new HashSet<>();
            String[] srcColumnDetails = {"", "0", "0", "0"};
            String[] tgtColumnDetails = {"", "0", "0", "0"};
            boolean srcSysEnvUpdateFlag = false;
            String[] updatedSysEnvDetails = Arrays.copyOf(sysEnvDetails, sysEnvDetails.length);

            for (sourceColumn source : sources) {
                if (duplicateSources.contains(source.getParent_name() + "." + source.getColumn())) {
                    continue;
                } else {
                    duplicateSources.add(source.getParent_name() + "." + source.getColumn());
                }
                String tableName = source.getParent_name();
                if (joinComponentMap.get(tableName) != null) {
                    tableName = joinComponentMap.get(tableName);
                }
                srcTableName = "".equals(srcTableName) ? tableName
                        : srcTableName + "\n" + tableName;
                srcColumnName = "".equals(srcColumnName) ? source.getColumn()
                        : srcColumnName + "\n" + source.getColumn();
            }
            if (joinComponentMap.get(tgtTableName) != null) {
                tgtTableName = joinComponentMap.get(tgtTableName);
            }
            String key = tgtTableName + "##" + tgtColumnName;
            String bRule = businessRuleMap.get(key);
            
            if(bRule!=null){
               if(bRule.equalsIgnoreCase("SYSDATE")){
                bRule="SYSDATE";
            } 
            }
            
            if (bRule != null) {
                businessRuleMap.remove(key);
            }

            /*source system name, target system name, source env name, target env name, scale, precision, data type*/
            srcTableName = cleanTableAndColumnNames(srcTableName);
            srcColumnName = cleanTableAndColumnNames(srcColumnName);
            tgtTableName = cleanTableAndColumnNames(tgtTableName);
            tgtColumnName = cleanTableAndColumnNames(tgtColumnName);
            
            srcTableName1=schemaModificationTable(srcTableName);
                if(srcTableName1!=null){
                    srcTableName=srcTableName1;
                }
                 String[] srcTgtDetails= {srcTableName, srcColumnName, tgtTableName, tgtColumnName, bRule};
            
            if(!srcColumnName.isEmpty() || !srcTableName.isEmpty()){
                specRows.add(addSpecRow(srcTgtDetails, srcColumnDetails, tgtColumnDetails, updatedSysEnvDetails)); 
            }else{
                System.out.println("SourceColumn And SourceTable IS EMPTY");
            }
           
        }
        if (businessRuleMap.size() > 0) {
            int j=0;
            ArrayList<MappingSpecificationRow> brSpecRows = new ArrayList<>();
            for (MappingSpecificationRow specRow : specRows) {
                
                String srcTableName = specRow.getSourceTableName();
                String srcColumnName = specRow.getSourceColumnName();
                String bRule = null;
                bRule = businessRuleMap.get(srcTableName + "##" + srcColumnName);
                if (bRule == null && srcTableName.split("\n").length > 1) {
                    for (int i = 0; i < srcTableName.split("\n").length; i++) {
                        String tablename = "";
                        String columnName = "";
                        if (srcColumnName.split("\n").length == srcTableName.split("\n").length) {
                            tablename = srcTableName.split("\n")[i];
                            columnName = srcColumnName.split("\n")[i];
                        }
                        bRule = businessRuleMap.get(tablename + "##" + columnName);
                        if (bRule != null) {
                            srcTableName = tablename;
                            srcColumnName = columnName;
                            break;
                        }
                    }
                }
//                if(j==0){
//                    for (Map.Entry<String,String> entry : businessRuleMap.entrySet()) { 
//                    if(entry.getValue().equalsIgnoreCase("SYSDATE")){
//                    specRow.setTargetTableName(entry.getKey().split("\\##")[0]);
//                    specRow.setTargetColumnName(entry.getKey().split("\\##")[1]);
//                    specRow.setSourceColumnName("");
//                    specRow.setSourceTableName("");
//                    specRow.setBusinessRule("SYSDATE");
//                   
//                    j++;
//            }
//    } 
// }
            
//                if (bRule != null) {
//                    String srcTableNameCleaned = cleanTableAndColumnNames(srcTableName);
//                    String srcColumnNameCleaned = cleanTableAndColumnNames(srcColumnName);
//                    
//                    if(!srcTableNameCleaned.isEmpty()||!srcColumnNameCleaned.isEmpty()){
//                    MappingSpecificationRow specRow1 = new MappingSpecificationRow();
//                    setSpecification(specRow1, sysEnvDetails);
//                    specRow1.setTargetTableName(srcTableNameCleaned);
//                    specRow1.setTargetColumnName(srcColumnNameCleaned);
//                    specRow1.setBusinessRule(bRule);
//                    brSpecRows.add(specRow1);
//                    businessRuleMap.remove(srcTableName + "##" + srcColumnName);
//                    if (businessRuleMap.isEmpty()) {
//                        break;
//                    } 
//                    }else{
//                        System.out.println("EMPTY Source ");
//                    }
//                   
//                }
            }
            specRows.addAll(brSpecRows);
        }
//        if (businessRuleMap.size() > 0) {
//            for (String key : businessRuleMap.keySet()) {
//                String srcTableName = key.split("##")[0];
//                String srcColumnName = key.split("##")[1];
//                String bRule = businessRuleMap.get(key);
//
//                String srcTableNameCleaned = cleanTableAndColumnNames(srcTableName);
//                String srcColumnNameCleaned = cleanTableAndColumnNames(srcColumnName);
//                
//                if(!srcTableNameCleaned.isEmpty()||!srcColumnNameCleaned.isEmpty()){
//                MappingSpecificationRow specRow1 = new MappingSpecificationRow();
//                setSpecification(specRow1, sysEnvDetails);
//                specRow1.setTargetTableName(srcTableNameCleaned);
//                specRow1.setTargetColumnName(srcColumnNameCleaned);
//                specRow1.setBusinessRule(bRule);
//                specRows.add(specRow1);
//                }else{
//                    System.out.println("EMPTY");
//                }
//               
//            }
//        }
        //For Madhus Requirement
        //specRows = RemoveResultsetComponent(specRows);
        MappingCreator_AF.businessRuleMap1=businessRuleMap;
        return specRows;
    }

    private MappingSpecificationRow addSpecRow(String[] srcTgtDetails, String[] sysEnvDetails) {
        MappingSpecificationRow mSpecRow = new MappingSpecificationRow();
        setSpecification(mSpecRow, sysEnvDetails);
        mSpecRow.setSourceTableName(srcTgtDetails[0]);
        mSpecRow.setSourceColumnName(srcTgtDetails[1]);
        mSpecRow.setTargetTableName(srcTgtDetails[2]);
        mSpecRow.setTargetColumnName(srcTgtDetails[3]);

        String bRule = srcTgtDetails[4];
        if (bRule != null) {
            mSpecRow.setBusinessRule(bRule);
        }
        return mSpecRow;
    }

    private MappingSpecificationRow addSpecRow(String[] srcTgtDetails, String[] srcColumnDetails, String[] tgtColumnDetails, String[] sysEnvDetails) {
        MappingSpecificationRow mSpecRow = new MappingSpecificationRow();
        setSpecification(mSpecRow, sysEnvDetails);
        mSpecRow.setSourceTableName(srcTgtDetails[0]);
        mSpecRow.setSourceColumnName(srcTgtDetails[1]);
        mSpecRow.setSourceColumnDatatype(srcColumnDetails[0]);
        mSpecRow.setSourceColumnLength(Integer.parseInt(srcColumnDetails[1]));
        mSpecRow.setSourceColumnScale(Integer.parseInt(srcColumnDetails[2]));
        mSpecRow.setSourceColumnPrecision(Integer.parseInt(srcColumnDetails[3]));

        mSpecRow.setTargetTableName(srcTgtDetails[2]);
        mSpecRow.setTargetColumnName(srcTgtDetails[3]);
        mSpecRow.setTargetColumnDatatype(tgtColumnDetails[0]);
        mSpecRow.setTargetColumnLength(Integer.parseInt(tgtColumnDetails[1]));
        mSpecRow.setTargetColumnScale(Integer.parseInt(tgtColumnDetails[2]));
        mSpecRow.setTargetColumnPrecision(Integer.parseInt(tgtColumnDetails[3]));

        String bRule = srcTgtDetails[4];
        if (bRule != null) {
            mSpecRow.setBusinessRule(bRule);
        }
        return mSpecRow;
    }

    private void prepareBusinessRules(relation relation) {
        if (!relation.getType().equals("businessrule")) {
            return;
        }
        List<sourceColumn> sources = relation.getSources();
        targetColumn target = relation.getTarget();
        String function = relation.getTarget().getFunction();
        String targetColumn = target.getParent_name() + "##" + target.getColumn();

        if (sources == null || sources.isEmpty()) {
            if (businessRuleMap.get(targetColumn) != null && !businessRuleMap.get(targetColumn).contains(function)) {
                function = businessRuleMap.get(targetColumn) + "\n" + function;
            }
            businessRuleMap.put(targetColumn, function);
            return;
        }
        for (sourceColumn source : sources) {
            String tableAliasName = tableAliasMap.get(source.getParent_id());
            String tableNameWithAlias = tableAliasName != null ? tableAliasName + "." + source.getColumn() : null;
            String replaceWith = source.getParent_name() + "." + source.getColumn();
            if (tableNameWithAlias != null && function.contains(tableNameWithAlias) && !function.contains(replaceWith)) {
                function = function.replace(tableNameWithAlias, replaceWith);
            }
        }
        if (businessRuleMap.get(targetColumn) != null && !businessRuleMap.get(targetColumn).contains(function)) {
            function = businessRuleMap.get(targetColumn) + "\n" + function;
        }
        businessRuleMap.put(targetColumn, function);
    }

    private void prepareAliasMap(List<table> tables) {
        for (table table : tables) {
            tableAliasMap.put(table.getId(), table.getAlias());
            System.out.println("Alias=>"+table.getAlias());
            System.out.println("Alias=>"+table.getId());
        }
    }

    private void prepareKeyValues(relation relation) {
        String[] reqRelations = {"join", "where", "groupBy", "orderBy"};
        List relationsList = Arrays.asList(reqRelations);
        if (!relationsList.contains(relation.getType())) {
            return;
        }
        if (relation.getType().equals("join")) {
            HashSet joinCondSet = keyValuesMap.get("JOIN_CONDITION");
            if (joinCondSet == null) {
                joinCondSet = new HashSet();
            }
            String joinCondition = prepareCondition(relation);
            joinCondition = prepareJoinCondition(relation, joinCondition);
            joinCondSet.add(joinCondition + DELIMITER + capFirstLetter(relation.getJoinType()));
            keyValuesMap.put("JOIN_CONDITION", joinCondSet);
        }
        if (relation.getType().equals("where")) {
            HashSet whereCondSet = keyValuesMap.get("WHERE_CONDITION");
            if (whereCondSet == null) {
                whereCondSet = new HashSet();
            }
            String condition = prepareCondition(relation);
            whereCondSet.add(condition+DELIMITER);
            keyValuesMap.put("WHERE_CONDITION", whereCondSet);
        }
        if (relation.getType().equals("groupBy")) {
            HashSet groupbyCondSet = keyValuesMap.get("GROUPBY_CONDITION");
            if (groupbyCondSet == null) {
                groupbyCondSet = new HashSet();
            }
            String condition = prepareCondition(relation);
            groupbyCondSet.add(condition+DELIMITER);
            keyValuesMap.put("GROUPBY_CONDITION", groupbyCondSet);
        }
        if (relation.getType().equals("orderBy")) {
            HashSet orderbyCondSet = keyValuesMap.get("ORDERBY_CONDITION");
            if (orderbyCondSet == null) {
                orderbyCondSet = new HashSet();
            }
            String condition = prepareCondition(relation);
            orderbyCondSet.add(condition+DELIMITER);
            keyValuesMap.put("ORDERBY_CONDITION", orderbyCondSet);
        }
    }

    private void setSpecification(MappingSpecificationRow specRow, String[] sysEnvDetails) {
        specRow.setSourceSystemName(sysEnvDetails[0]);
        specRow.setSourceSystemEnvironmentName(sysEnvDetails[1]);
        specRow.setTargetSystemName(sysEnvDetails[2]);
        specRow.setTargetSystemEnvironmentName(sysEnvDetails[3]);
    }

    public LinkedHashMap<String, HashSet<String>> getKeyValuesMap() {
        return keyValuesMap;
    }

    private String prepareJoinCondition(relation relation, String condition) {
        targetColumn target = relation.getTarget();
        if (target == null) {
            return condition;
        }
        String tableAliasNames = tableAliasMap.get(target.getParent_id());
        String columnName = target.getColumn();
        boolean flag = false;
        String toBeReplaceWith = target.getParent_name() + "." + columnName;
        if (condition.contains(toBeReplaceWith)) {
            return condition;
        }
        if (tableAliasNames != null) {
            //If multiple alias names with comma separated
            List<String> aliasList = Arrays.asList(tableAliasNames.split(","));
            for (String alias : aliasList) {
                String tableNameWithAlias = alias + "." + columnName;
                if (condition.contains(tableNameWithAlias)) {
                    condition = condition.replace(tableNameWithAlias, toBeReplaceWith);
                    flag = true;
                    break;
                }
            }
        }

//        if (!flag && condition.contains(columnName)) {
//            condition = condition.replace(columnName,
//                    target.getParent_name() + "." + columnName);
//        }
        return condition;
    }

    private String prepareCondition(relation relation) {

        String condition = relation.getCondition();
        List<sourceColumn> sources = relation.getSources();
        if (sources != null) {
            HashSet<String> srcDuplicates = new HashSet<>();
            for (sourceColumn source : sources) {

                String tableAliasNames = tableAliasMap.get(source.getParent_id());
                String columnName = source.getColumn();
                boolean flag = false;
                String toBeReplaceWith = source.getParent_name() + "." + columnName;
                if (srcDuplicates.contains(toBeReplaceWith) || condition.contains(toBeReplaceWith)) {
                    continue;
                }
                if (tableAliasNames != null) {
                    //If multiple alias names with comma separated
                    List<String> aliasList = Arrays.asList(tableAliasNames.split(","));
                    for (String alias : aliasList) {
                        String toBeReplaced = alias + "." + columnName;
                        condition=" "+condition;
                        if (condition.contains(" "+toBeReplaced)) {
                            condition = condition.replace(" "+toBeReplaced, " "+toBeReplaceWith);
                           
                            if(condition.contains(" "+"RESP_"+toBeReplaced)){
                            condition = condition.replace(" "+"RESP_"+toBeReplaced, " "+toBeReplaceWith);
                        }
                             flag = true;
                            break;
                        }else{
                            if (condition.contains(toBeReplaced)) {
                                if(condition.contains("RESP_"+toBeReplaced)){
                                    condition = condition.replace("RESP_"+toBeReplaced , toBeReplaceWith);
                                }else{
                                    condition = condition.replace(toBeReplaced, toBeReplaceWith);
                                }
                            
                            flag = true;
                            break;
                        }
                            }
                        
                    }
                }
                
//                if (!flag && condition.contains(columnName)) {
//                    condition = condition.replace(columnName,
//                            toBeReplaceWith);
//                }
                srcDuplicates.add(toBeReplaceWith);
            }
        }
        return condition;
    }

    private void preparejoinComponent(LinkedHashMap<String, HashSet<String>> srcTgtCmpnt) {

        HashSet<String> joins = keyValuesMap.get("JOIN_CONDITION");
        if (joins == null) {
            return;
        }

        for (String join : joins) {
            if (join.split("=").length <= 1) {
                continue;
            }
            String source1 = getParentName(join.split("=")[0]);
            String source2 = getParentName(join.split("=")[1]);
            for (String targetTable : srcTgtCmpnt.keySet()) {
                HashSet<String> sources = srcTgtCmpnt.get(targetTable);
                if (sources.contains(source1) && sources.contains(source2)) {
                    joinComponentMap.put(targetTable,  targetTable);
                    break;
                }
            }
        }
    }

    private String getParentName(String joinCond) {
        if (joinCond.split("\\.").length > 1) {
            joinCond = joinCond.substring(0, joinCond.lastIndexOf("."));
        }
        return joinCond.trim();
    }

    private String capFirstLetter(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    private void prepareSrcTgtMap(relation relation, LinkedHashMap<String, HashSet<String>> srcTgtCmpnt) {

        if (!relation.getType().equals("lineage") || relation.getTarget() == null
                || relation.getSources() == null) {
            return;
        }
        String tgtCmpnt = relation.getTarget().getParent_name();
        for (sourceColumn srcColumn : relation.getSources()) {
            String srcCmpnt = srcColumn.getParent_name();
            HashSet srcSet = srcTgtCmpnt.get(tgtCmpnt);
            if (srcSet == null) {
                srcSet = new HashSet();
            }
            srcSet.add(srcCmpnt);
            srcTgtCmpnt.put(tgtCmpnt, srcSet);
        }
    }

    private LinkedHashMap<String, String> ModifyBusinessRuleMap() {
        Set<String> keyset = businessRuleMap.keySet();
        LinkedHashMap<String, String> brRuleMap = new LinkedHashMap<>();
        for (String key : keyset) {
            String tableName = key.split("##")[0];
            String columnName = key.split("##")[1];
            String value = businessRuleMap.get(key);
            String newCompName = joinComponentMap.get(tableName);
            if (newCompName != null) {
                brRuleMap.put(newCompName + "##" + columnName, value);
            } else {
                brRuleMap.put(key, value);
            }
        }
        return brRuleMap;
    }

    private ArrayList<MappingSpecificationRow> RemoveResultsetComponent(ArrayList<MappingSpecificationRow> specRows) {
        ArrayList<MappingSpecificationRow> specRowList = new ArrayList<>();
        for (MappingSpecificationRow mappingSpecificationRow : specRows) {
            String targetTable = mappingSpecificationRow.getTargetTableName();
            if (!targetTable.startsWith("RS_")) {
                specRowList.add(mappingSpecificationRow);
            }
        }
        return specRowList;
    }

    public static String cleanTableAndColumnNames(String str) {

        str = str.replace("[", "").replace("]", "");
        str = str.replace("`", "");
        str = str.replace("'", "");
        str = str.replace("\"", "");

        return str;
    }

    private void removeDupilcatesWithAliasName(List<table> tables) {
        int i=0;
       for(table table1:tables){
           String tableName=table1.getName();
          
           List columnDetails=table1.getColumns();
           for(table table2:tables){
               String tableName1=table2.getName();
               if(tableName1.split(".").length==2){
                   String tableNameWithOutAlias=tableName1.split(".")[1];
                   if(tableNameWithOutAlias.equalsIgnoreCase(tableName)){
                       tables.remove(i);
                   }
               }
           }
            i++;
       }
    }

    private void prepareSchemaMap(List<table> tables) {
       for (table table : tables) {
           if(table.getSchema()!=null){
              tableSchemaMap.put(table.getId(), table.getSchema()+"#"+table.getName()); 
           } 
        }   
    }
    
    private String schemaModificationTable(String srcTableName){
          for (Map.Entry<String,String> entry : tableSchemaMap.entrySet()) { 
            String tableName1=entry.getValue();
            String tableName2=tableName1.split("#")[1].split("\\.")[1];
            if(tableName2.equalsIgnoreCase(srcTableName)){
                return tableName1.split("#")[1];
            }
        } 
           return null;
       }
}
