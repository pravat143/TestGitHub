/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.sqlparser.v3;

import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.erwin.dataflow.model.xml.dataflow;
import com.erwin.util.XML2Model;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author Trinesh
 */
public class MappingCreator_Trinesh {

    private LinkedHashMap<String, HashSet<String>> keyValuesDeailsMap;

    public LinkedHashMap<String, HashSet<String>> getKeyValuesDeailsMap() {
        return this.keyValuesDeailsMap;
    }

    public static void main(String[] args) {

        String fileDirectoryPath = "F:\\Test";
        String[] sysEnvDetails = {"SrcSystem", "SrcEnv", "TgtSystem", "TgtEnv"};
        File dir = new File(fileDirectoryPath);
        search(".*\\.sql", dir, sysEnvDetails);

    }

    public dataflow getDataflowFromSql(String sqltext, String fileName, String[] sysenvDetails) {

        try {

//            TGSqlParser sqlparser = isQueryParsable(sqltext);
            TGSqlParser sqlparser = isQueryParsable(sqltext, EDbVendor.dbvmssql);
            if (sqlparser == null) {
                System.out.println("Query is Not able to parse");
                return null;
            } else {
                System.out.println("Query is Compatible to " + sqlparser.getDbVendor());
            }
            DataFlowAnalyzer dlineage = new DataFlowAnalyzer(sqltext, fileName,EDbVendor.dbvmssql, false);
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
        try {

            File inputFile = new File(inputFilePath);
            String fileName = getFileName(inputFile.getName());
            String sqltext = FileUtils.readFileToString(inputFile, "UTF-8");

            dataflow dtflow = getDataflowFromSql(sqltext, fileName, sysenvDetails);
            if (dtflow == null) {
                return new ArrayList<>();
            }
            RelationAnalyzer relationAnalyzer = new RelationAnalyzer();

            ArrayList<MappingSpecificationRow> mapSpecRows = relationAnalyzer.analyzeRelations(dtflow, sysenvDetails);
            this.keyValuesDeailsMap = relationAnalyzer.getKeyValuesMap();
            int i = 1;
            for (MappingSpecificationRow mspecRow : mapSpecRows) {
                i = i + 1;
                System.out.println(i + ")" + mspecRow.getSourceTableName() + "====>" + mspecRow.getSourceColumnName() + "====>"
                        + mspecRow.getBusinessRule() + "===>" + mspecRow.getTargetTableName() + "====>" + mspecRow.getTargetColumnName());
            }

            System.out.println("keyValuesMap ===> " + this.keyValuesDeailsMap);
            return new ArrayList<>();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public Mapping getMappingObject(String inputFilePath, String[] sysenvDetails, int projectId, int subjectId) {
        Mapping mapping = null;
        try {

            File inputFile = new File(inputFilePath);
            String fileName = getFileName(inputFilePath);
            String sqltext = FileUtils.readFileToString(inputFile, "UTF-8");

            dataflow dtflow = getDataflowFromSql(sqltext, fileName, sysenvDetails);
            if (dtflow == null) {
                return null;
            }
            System.out.println("keyValuesMap ===> " + this.keyValuesDeailsMap);

            mapping = new Mapping();
            mapping.setMappingName(fileName);
            mapping.setProjectId(projectId);
            mapping.setSubjectId(subjectId);
            mapping.setMappingSpecifications(null);
            mapping.setSourceExtractQuery(sqltext);

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

    public static void search(String pattern, File folder, String[] sysEnvDetails) {
        for (File f : folder.listFiles()) {
            if (f.isDirectory()) {
                search(pattern, f, sysEnvDetails);
            }
            if (f.isFile()) {
                if (f.getName().matches(pattern)) {
                    System.out.println("------------- FileName is--->" + f.getAbsolutePath() + "<----------------");
                    MappingCreator_Trinesh mappingCreator = new MappingCreator_Trinesh();
                    mappingCreator.getMappingSpecList(f.getAbsolutePath(), sysEnvDetails);
                }
            }

        }
    }

    private String getFileName(String name) {
        if (name == null || name.lastIndexOf(".") < 0) {
            return "";
        }
        return name.substring(0, name.lastIndexOf("."));
    }

}
