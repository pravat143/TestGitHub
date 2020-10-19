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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
public class ErwinSQLWrapper1 {

    /**
     * Build List of queries from file object.
     * <br>
     * It will add defaultExtremeTargetName as extreme target if
     * addDefaultExtremeTarget is true. This happens only when one component
     * available without target.
     *
     * @param file Object
     * @param dbVendor String
     * @param addDefaultExtremeTarget boolean
     * @param defaultExtremeTargetName String
     * @return List of query strings
     */
    /**
     * Build List of queries from sql Query.
     * <br>
     * It will add defaultExtremeTargetName as extreme target if
     * addDefaultExtremeTarget is true. This happens only when one component
     * available without target.
     *
     * @param sqlContent String
     * @param dbVendor String
     * @param addDefaultExtremeTarget boolean
     * @param defaultExtremeTargetName String
     * @param folderName
     * @return List of query strings
     */
    public static Set<String> getAllStatementsForSqlFile(String inputfile, String dbVender) {
        Set<String> storeprocfile = null;
        try {

            long t = System.currentTimeMillis();

            EDbVendor dbVendor = null;
            List<EDbVendor> dbvenderlist = Arrays.asList(EDbVendor.values());

            for (EDbVendor vendor : dbvenderlist) {

                TGSqlParser sqlparser2 = new TGSqlParser(vendor);
                sqlparser2.sqlfilename = inputfile;
                if (sqlparser2.getrawsqlstatements() == 0) {
                    dbVendor = vendor;
                    break;

                }

            }

            TGSqlParser sqlparser = new TGSqlParser(dbVendor);

            sqlparser.sqlfilename = inputfile;

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

        } catch (Exception e) {
            e.printStackTrace();
            storeprocfile = null;
        }
        return storeprocfile;
    }

    public static Set<String> getAllStatementsForSqlData(String inputfile, String dbVender) {
        Set<String> storeprocfile = null;
        long t = System.currentTimeMillis();

        EDbVendor dbVendor = null;
        List<EDbVendor> dbvenderlist = Arrays.asList(EDbVendor.values());
        for (EDbVendor vendor : dbvenderlist) {

            TGSqlParser sqlparser2 = new TGSqlParser(vendor);
            sqlparser2.sqltext = inputfile;
            if (sqlparser2.getrawsqlstatements() == 0) {
                dbVendor = vendor;
                break;

            }

        }

        TGSqlParser sqlparser = new TGSqlParser(dbVendor);

        sqlparser.sqltext = inputfile;

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

}
