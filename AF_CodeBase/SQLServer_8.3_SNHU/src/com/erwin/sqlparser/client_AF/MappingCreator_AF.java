/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.sqlparser.client_AF;

import com.ads.api.beans.common.Node;
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.util.MappingManagerUtil;
import com.erwin.dataflow.model.xml.dataflow;
import com.erwin.sqlparser.wrapper.parser.ErwinSQLWrapper;
import com.erwin.util.XML2Model;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.icc.util.RequestStatus;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.pp.para.GFmtOpt;
import gudusoft.gsqlparser.pp.para.GFmtOptFactory;
import gudusoft.gsqlparser.pp.stmtformatter.FormatterFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author Trinesh
 */
public class MappingCreator_AF {

    private static LinkedHashMap<String, HashSet<String>> keyValuesDeailsMap;
    public static EDbVendor dbVendor = EDbVendor.dbvoracle;
    public static LinkedHashMap<String, String> businessRuleMap1;
    
    public LinkedHashMap<String, HashSet<String>> getKeyValuesDeailsMap() {
        return this.keyValuesDeailsMap;
    }
    
    public static void main(String[] args) {
        
//        String fileDirectoryPath = "D:\\Projects\\SNHU\\ParsedFiles";
//        String fileDirectoryPath = "D:\\Projects\\SNHU\\Issues";
//        String fileDirectoryPath = "D:\\Projects\\SNHU\\Test";
        String fileDirectoryPath = "D:\\My WorkSpace\\New folder";
        String[] sysEnvDetails = {"SrcSystem", "SrcEnv", "TgtSystem", "TgtEnv"};
        String vendor = "oracle";
        MappingCreator_AF creator = new MappingCreator_AF();
        dbVendor = creator.getDBVendor(vendor);
        File dir = new File(fileDirectoryPath);
        List<Mapping> mappinglist = creator.search(".*\\.sql", dir, sysEnvDetails,1,1);

    }

    public String createMappingForOracle(String dir1, String sysEnvDetails1, int projectId, int subjectId, String vendor,MappingManagerUtil maputil) {
        StringBuilder sb = new StringBuilder();
        List<Mapping> mappinglist=null;
        File dir=new File(dir1);
        StringBuffer buffer=new StringBuffer();
        String[] sysEnvDetails=sysEnvDetails1.split("#");
        List<MappingSpecificationRow>mapSpRow=new ArrayList<>();
        try {
            MappingCreator_AF creator = new MappingCreator_AF();
            dbVendor = creator.getDBVendor(vendor);
             //mappinglist = creator.search(".*\\.sql", dir, sysEnvDetails,projectId,subjectId);
             mappinglist = creator.search(".*\\.sql", dir, sysEnvDetails,projectId);
            for (Mapping mapping : mappinglist) {
                List<Mapping>jsMappings=new ArrayList();
               MappingSpecificationRow row1=new MappingSpecificationRow();
               ArrayList newRow1=new ArrayList();
               
               for (Map.Entry<String,String> entry : businessRuleMap1.entrySet()) { 
                    if(entry.getValue().equalsIgnoreCase("SYSDATE")){
                    row1.setTargetTableName(entry.getKey().split("\\##")[0]);
                    row1.setTargetColumnName(entry.getKey().split("\\##")[1]);
                    row1.setSourceColumnName("");
                    row1.setSourceTableName("");
                    row1.setBusinessRule("SYSDATE");
                   
                    newRow1.addAll(mapping.getMappingSpecifications());
                    newRow1.add(row1);
                    //newRow1.addAll(mapping.getMappingSpecifications());
                    mapping.setMappingSpecifications(newRow1);
                    
            }
    } 
               String mapName1=mapping.getMappingName();
               mapping.setMappingName(mapName1.substring(0,mapName1.length()-4));
               jsMappings.add(mapping);
                ObjectMapper objectMapper = new ObjectMapper();
                String mappingJson = objectMapper.writeValueAsString(jsMappings);

                return mappingJson;
//                String mapName=mapping.getMappingName();
//                mapName=mapName.replaceAll("[^\\w\\s]+","");
//                mapping.setMappingName(mapName);
//                RequestStatus value= maputil.createMapping(mapping);
//                buffer.append(value.getStatusMessage());
//                buffer.append("\n");
           }
        } catch (Exception e) {
        }
        return buffer.toString();
    }

    public dataflow getDataflowFromSql(String sqltext, String fileName, String[] sysenvDetails) {

        try {

//            TGSqlParser sqlparser = isQueryParsable(sqltext);
            TGSqlParser sqlparser = isQueryParsable(sqltext, dbVendor);
            if (sqlparser == null) {
                System.out.println("Query is Not able to parse");
                return null;
            } else {
                System.out.println("Query is Compatible to " + sqlparser.getDbVendor());
            }
            DataFlowAnalyzer_AF dlineage = new DataFlowAnalyzer_AF(sqltext, fileName, dbVendor, false);
            dlineage.setShowJoin(true);
            dlineage.setIgnoreRecordSet(false);

            StringBuffer errorBuffer = new StringBuffer();
            String result = dlineage.generateDataFlow(errorBuffer);
            
//            File xmlFile = new File("C:\\Users\\TrineshVanguri\\Desktop\\Version8_3.xml");
//            FileUtils.writeStringToFile(xmlFile, result, "UTF-8");
            return XML2Model.loadXML(dataflow.class, result);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ArrayList<MappingSpecificationRow> getMappingSpecList(String inputFilePath, String[] sysenvDetails) {
        ArrayList<MappingSpecificationRow> mapSpecRows = null;
        try {

            File inputFile = new File(inputFilePath);
            String fileName = getFileName(inputFile.getName());
            String sqltext = FileUtils.readFileToString(inputFile);

            sqltext = ErwinSQLWrapper.removeUnparsedDataFromQuery(sqltext);
            //Create a New file

            dataflow dtflow = getDataflowFromSql(sqltext, fileName, sysenvDetails);
            if (dtflow == null) {
                return new ArrayList<>();
            }
            RelationAnalyzer_AF relationAnalyzer = new RelationAnalyzer_AF();

            mapSpecRows = relationAnalyzer.analyzeRelations(dtflow, sysenvDetails);
            this.keyValuesDeailsMap = relationAnalyzer.getKeyValuesMap();
            int i = 1;
            for (MappingSpecificationRow mspecRow : mapSpecRows) {
                i = i + 1;
                System.out.println(i + ")" + mspecRow.getSourceTableName() + "====>" + mspecRow.getSourceColumnName() + "====>"
                        + mspecRow.getBusinessRule() + "===>" + mspecRow.getTargetTableName() + "====>" + mspecRow.getTargetColumnName());
            }

            System.out.println("keyValuesMap ===> " + this.keyValuesDeailsMap);
            //return new ArrayList<>();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return mapSpecRows;
    }

    public Mapping getMappingObject(String inputFilePath, String[] sysenvDetails, int projectId, int subjectId) {
        Mapping mapping = null;
        try {

            File inputFile = new File(inputFilePath);
            String fileName = getFileName(inputFilePath);
            String sqltext = FileUtils.readFileToString(inputFile, "UTF-8");

            dataflow dtflow = getDataflowFromSql(sqltext, fileName, sysenvDetails);

//            if (dtflow == null) {
//                return null;
//            }
           
            ArrayList<MappingSpecificationRow> mapspeclist = getMappingSpecList(inputFilePath, sysenvDetails);
            mapping = new Mapping();
            mapping.setMappingName(inputFile.getName());
            mapping.setProjectId(projectId);
           //mapping.setSubjectId(subjectId);
            //mapping.setMappingSpecifications(null);
            mapping.setSourceExtractQuery(sqltext);
            mapping.setMappingSpecifications(mapspeclist);

        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return mapping;

    }

    private TGSqlParser isQueryParsable(String sqltext) {
        int parsedResult = -1;
        TGSqlParser sqlparser = null;
        List<EDbVendor> dbvenderlist = Arrays.asList(EDbVendor.values());
        for (EDbVendor vendor : dbvenderlist) {
            sqlparser = new TGSqlParser(vendor);
            sqlparser.sqltext = sqltext;
            parsedResult = sqlparser.parse();
            if (parsedResult == 0) {
                break;
            } else {
                sqlparser = null;
            }
        }
        return sqlparser;
    }

    private TGSqlParser isQueryParsable(String sqltext, EDbVendor vendor) {
        TGSqlParser sqlparser = new TGSqlParser(vendor);
        sqlparser.sqltext = sqltext;
        int parsedResult = sqlparser.parse();
        if (parsedResult == 0) {
            return sqlparser;
        } else {
            return null;
        }
    }

    public List<Mapping> search(String pattern, File folder, String[] sysEnvDetails, int projectId, int subjectId) {
        List<Mapping> mappinglist = new ArrayList();
        try {
            for (File f : folder.listFiles()) {
                if (f.isDirectory()) {
                    search(pattern, f, sysEnvDetails, projectId, subjectId);
                }
                if (f.isFile()) {
                    if (f.getName().matches(pattern) || f.getName().matches(".*\\.dml")) {
                        System.out.println("------------- FileName is--->" + f.getAbsolutePath() + "<----------------");
                        MappingCreator_AF mappingCreator = new MappingCreator_AF();
                        Mapping mapping = mappingCreator.getMappingObject(f.getAbsolutePath(), sysEnvDetails, projectId, projectId);
                        mappinglist.add(mapping);
                    }
                }

            }
        } catch (Exception e) {
        }
        return mappinglist;
    }

    private String getFileName(String name) {
        if (name == null || name.lastIndexOf(".") < 0) {
            return "";
        }
        return name.substring(0, name.lastIndexOf("."));
    }

    public String formatSQLQuery(String sqlContent) {
        try {
            //format query
            TGSqlParser gSqlParser = new TGSqlParser(dbVendor);
            gSqlParser.setSqltext(sqlContent);
            GFmtOpt option = GFmtOptFactory.newInstance();
            sqlContent = FormatterFactory.pp(gSqlParser, option);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sqlContent;
    }

    public static String removeComments(String sql) {
        String regexPattern = "(--.*)|(((/\\*\\*)+?[\\w\\W]+?(\\*\\*/)+))";
        sql = sql.replaceAll(regexPattern, "");
        /*Removing extra *\/ with empty */
        regexPattern = "(--.*)|(((/\\*)+?[\\w\\W]+?(\\*/)+))";
        sql = sql.replaceAll(regexPattern, "");
        return sql;

    }

    public EDbVendor getDBVendor(String dbName) {
        dbVendor = EDbVendor.dbvmssql;
        if ("oracle".equalsIgnoreCase(dbName)) {
            dbVendor = EDbVendor.dbvoracle;
        } else if ("mssql".equalsIgnoreCase(dbName)) {
            dbVendor = EDbVendor.dbvmssql;
        } else if ("postgresql".equalsIgnoreCase(dbName)) {
            dbVendor = EDbVendor.dbvpostgresql;
        } else if ("redshift".equalsIgnoreCase(dbName)) {
            dbVendor = EDbVendor.dbvredshift;
        } else if ("odbc".equalsIgnoreCase(dbName)) {
            dbVendor = EDbVendor.dbvodbc;
        } else if ("mysql".equalsIgnoreCase(dbName)) {
            dbVendor = EDbVendor.dbvmysql;
        } else if ("netezza".equalsIgnoreCase(dbName)) {
            dbVendor = EDbVendor.dbvnetezza;
        } else if ("firebird".equalsIgnoreCase(dbName)) {
            dbVendor = EDbVendor.dbvfirebird;
        } else if ("access".equalsIgnoreCase(dbName)) {
            dbVendor = EDbVendor.dbvaccess;
        } else if ("ansi".equalsIgnoreCase(dbName)) {
            dbVendor = EDbVendor.dbvansi;
        } else if ("generic".equalsIgnoreCase(dbName)) {
            dbVendor = EDbVendor.dbvgeneric;
        } else if ("greenplum".equalsIgnoreCase(dbName)) {
            dbVendor = EDbVendor.dbvgreenplum;
        } else if ("hive".equalsIgnoreCase(dbName)) {
            dbVendor = EDbVendor.dbvhive;
        } else if ("sybase".equals(dbName)) {
            dbVendor = EDbVendor.dbvsybase;
        } else if ("hana".equals(dbName)) {
            dbVendor = EDbVendor.dbvhana;
        } else if ("impala".equals(dbName)) {
            dbVendor = EDbVendor.dbvimpala;
        } else if ("dax".equals(dbName)) {
            dbVendor = EDbVendor.dbvdax;
        } else if ("vertica".equals(dbName)) {
            dbVendor = EDbVendor.dbvvertica;
        } else if ("couchbase".equals(dbName)) {
            dbVendor = EDbVendor.dbvcouchbase;
        } else if ("snowflake".equals(dbName)) {
            dbVendor = EDbVendor.dbvsnowflake;
        } else if ("openedge".equals(dbName)) {
            dbVendor = EDbVendor.dbvopenedge;
        } else if ("informix".equals(dbName)) {
            dbVendor = EDbVendor.dbvinformix;
        } else if ("teradata".equals(dbName)) {
            dbVendor = EDbVendor.dbvteradata;
        } else if ("mdx".equals(dbName)) {
            dbVendor = EDbVendor.dbvmdx;
        } else if ("db2".equals(dbName)) {
            dbVendor = EDbVendor.dbvdb2;
        }
        return dbVendor;
    }
    
    public  LinkedHashMap<String,String> getKeyvalueJson(){
        LinkedHashMap<String,String>keyValueMap=new LinkedHashMap<>();
       
        
        for (Map.Entry<String,HashSet<String>> entry :keyValuesDeailsMap.entrySet())  {
          HashSet<String> value=entry.getValue();
          int i=1;
          for(String extprp1 : value){
              String[] property =extprp1.split("\\#@ERWIN@#");
              
              for(int k=0;k< property.length;k++){
                  String propertyValue=property[k];
                  if(!propertyValue.equalsIgnoreCase("Join")){
                     keyValueMap.put(entry.getKey()+" "+i,propertyValue.trim().replace("\t", " ").replaceAll("( )+", " "));
                     i++;  
                  }
                 
              }
          }
           
        }
            
        return keyValueMap;
    }
    public List<Mapping> search(String pattern, File folder, String[] sysEnvDetails, int projectId) {
        List<Mapping> mappinglist = new ArrayList();
        try {
                    if (folder.getName().matches(pattern) || folder.getName().matches(".*\\.dml")) {
                        System.out.println("------------- FileName is--->" + folder.getAbsolutePath() + "<----------------");
                        MappingCreator_AF mappingCreator = new MappingCreator_AF();
                        Mapping mapping = mappingCreator.getMappingObject(folder.getAbsolutePath(), sysEnvDetails, projectId, projectId);
                        mappinglist.add(mapping);
                    }
                

            
        } catch (Exception e) {
        }
        return mappinglist;
    }
    
     public void getClear() {
        clearStaticvariable();

    }

    private void clearStaticvariable() {
        keyValuesDeailsMap.clear();
    }

    private void GettingXmlFromQuery(String result, String sqlFilepath, String writeXml,String fileName) throws IOException {
        File xmlFile = new File(writeXml);
        FileUtils.writeStringToFile(xmlFile, result, "UTF-8");
    }
}
