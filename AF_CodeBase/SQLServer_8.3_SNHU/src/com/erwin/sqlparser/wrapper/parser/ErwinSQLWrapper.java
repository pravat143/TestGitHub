/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.sqlparser.wrapper.parser;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.stmt.TCreateFunctionStmt;
import gudusoft.gsqlparser.stmt.TCreateIndexSqlStatement;
import gudusoft.gsqlparser.stmt.TCreateProcedureStmt;
import gudusoft.gsqlparser.stmt.TCreateSynonymStmt;
import gudusoft.gsqlparser.stmt.TCreateViewSqlStatement;
import gudusoft.gsqlparser.stmt.mssql.TMssqlCreateFunction;
import gudusoft.gsqlparser.stmt.mssql.TMssqlCreateProcedure;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreateFunction;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreateProcedure;
import gudusoft.gsqlparser.stmt.teradata.TTeradataLock;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author NarsimuluKondapaka
 */
/**
 *
 * This class methods are to parse procedure/any query to gudusoft able to parse
 * query.
 *
 */
public class ErwinSQLWrapper {

    public static String useLine = "";
    public static StringBuilder exceptionBuilder;
    
    public static void main(String[] args) {
        String filePath="D:\\My WorkSpace\\Oracle_Reverse_AF__v1.0\\New folder (2)\\ESH_Closed_demands_detail_responder copy.sql";
        getAllStatementsForSqlFile(filePath, "Oracle");
    }

    public static Set<String> getAllStatementsForSqlFile(String inputfile, String dbVender) {
        try {
            String sqlContent = FileUtils.readFileToString(new File(inputfile), "UTF-8");
            return getAllStatements(sqlContent, dbVender);
        } catch (IOException ex) {
            Logger.getLogger(ErwinSQLWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return new HashSet();
    }

    public static Set<String> getAllStatements(String sqlContent, String dbVender) {
        Set<String> storeprocfile = null;
        long t = System.currentTimeMillis();

        EDbVendor dbVendor = null;
        List<EDbVendor> dbvenderlist = Arrays.asList(EDbVendor.values());
        for (EDbVendor vendor : dbvenderlist) {
            TGSqlParser sqlparser2 = new TGSqlParser(vendor);
            sqlparser2.sqltext = sqlContent;
            if (sqlparser2.getrawsqlstatements() == 0) {
                dbVendor = vendor;
                break;
            }
        }

        TGSqlParser sqlparser = new TGSqlParser(dbVendor);
        sqlparser.sqltext = sqlContent;
        int ret = sqlparser.getrawsqlstatements();
        if (ret == 0) {
            storeprocfile = new LinkedHashSet();
            for (int i = 0; i < sqlparser.sqlstatements.size(); i++) {
                TCustomSqlStatement customStmt = sqlparser.sqlstatements.get(i);
                if (customStmt instanceof TCreateProcedureStmt || customStmt instanceof TMssqlCreateProcedure) {
                    storeprocfile.add(customStmt.toString());
                } else if (customStmt instanceof TCreateFunctionStmt || customStmt instanceof TMssqlCreateFunction) {
                    storeprocfile.add(customStmt.toString());
                } else if (customStmt instanceof TCreateViewSqlStatement) {
                    storeprocfile.add(customStmt.toString());
                } else if (customStmt instanceof TCreateSynonymStmt) {
                    storeprocfile.add(customStmt.toString());
                } else if (customStmt instanceof TTeradataLock) {
                    storeprocfile.add(customStmt.toString());
                } else if (customStmt instanceof TPlsqlCreateFunction) {
                    storeprocfile.add(customStmt.toString());
                } else if (customStmt instanceof TPlsqlCreateProcedure) {
                    storeprocfile.add(customStmt.toString());
                } else if (customStmt instanceof TCreateIndexSqlStatement) {
                    storeprocfile.add(customStmt.toString());
                } else {
                    storeprocfile.add(customStmt.toString());
                }
            }

        } else {
            System.out.println("error----" + sqlparser.getErrormessage());
        }
        return storeprocfile;
    }

    private String getProcedureName(String procStmt) {
        try {
            if (procStmt != null && !procStmt.isEmpty()) {
                String firstLine = procStmt.substring(0, procStmt.indexOf("\n"));
//                firstLine = firstLine.replace("\r", "").replace("\n", "");
                firstLine = firstLine.replaceAll("\t", " ").replaceAll("( )+", " ");
                if (firstLine.split(" ").length > 2) {
                    String procName = firstLine.split(" ")[2];
                    procName = procName.replace("[", "").replace("]", "");
                    procName = procName.substring(procName.lastIndexOf(".") + 1);
                    return procName;
                }
            }
        } catch (Exception e) {
        }
        return "";
    }

    public static String removeUnparsedDataFromQuery(String sqlText) {
        exceptionBuilder = new StringBuilder();
        try {
            if (sqlText.trim().startsWith("USE")) {
                useLine = sqlText.split(" ")[1].trim().replace("[", "").replace("]", "").toUpperCase();
            }
        } catch (Exception e) {
            e.printStackTrace();
            exceptionBuilder.append("Exception Details: " + e + "\n");
        }
        String parsedData = "";
        boolean flag = true;
        try {

            String spiltSqlFileData[] = sqlText.split("\n");

            for (String data : spiltSqlFileData) {

                if (StringUtils.isBlank(data)) {
                    continue;
                }
                // the 1st space in the downline is some special space character from the query that's why the query is not parsing so we replaced
                // the special space character with normal space character in the downline
                data = data.replaceAll(" ", " ");
                data = data.toUpperCase().replace("FROM ISNULL", "+ ISNULL");
                data = data.replaceAll("&","AND ");
                try {
                    if (data.startsWith("USE")) {
                        useLine = data.split(" ")[1].trim().replace("[", "").replace("]", "").toUpperCase();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    exceptionBuilder.append("Exception Details: " + e + "\n");
                }

                if (data.toUpperCase().contains("OPTION") && data.toUpperCase().contains("USE")) {

                    try {
//                        data = data.substring(0, data.toUpperCase().indexOf("OPTION"));

                        data = data.toUpperCase().replace("OPTION", ";--OPTION");
//                        if (flag) {
                        parsedData = parsedData + data + "\n";
//                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

//                    continue;
                } else if (!data.trim().toUpperCase().startsWith("CREATE") && data.toUpperCase().contains("INDEX") && data.toUpperCase().contains("CLUSTERED")) {
                    continue;
                } else if (data.trim().toUpperCase().startsWith("ALTER TABLE")) {
                    flag = false;

                    continue;
                } else if (data.trim().toUpperCase().contains("PERCENTILE_DISC(.5)")) {
                    continue;
                } else {

                    try {
                        if (data.contains("CASE") && data.contains("?") || data.contains("Â€‹") || data.contains("â€‹")) {//Â€‹
                            data = data.replaceAll("\\?", "").replaceAll("Â€‹", "").replaceAll(" CASE", "CASE").replaceAll("â€‹", "");
                        }

                        data = data.toUpperCase().replaceAll("AT TIME ZONE 'EASTERN STANDARD TIME'", "");
                        data = data.toUpperCase().replaceAll("FOR SYSTEM_TIME ALL", "");
                        data = data.toUpperCase().replaceAll("RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", "");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

//                    }
                    if (data.toUpperCase().contains("SELECT") || data.toUpperCase().contains("UPDATE")
                            || data.toUpperCase().contains("DELETE") || data.toUpperCase().contains("INSERT")
                            || data.toUpperCase().contains("SET") || data.toUpperCase().contains("DECLARE")
                            || data.toUpperCase().contains("PRINT") || data.toUpperCase().contains("TRUNCATE") || data.toUpperCase().contains("IF EXISTS")) {
                        flag = true;
                    }
                    if (flag) {
                        parsedData = parsedData + data + "\n";
                    }

                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            exceptionBuilder.append("Exception Details: " + e + "\n");

        }
        return parsedData;
    }

}
