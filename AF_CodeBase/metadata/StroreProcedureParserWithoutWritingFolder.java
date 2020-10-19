//package com.erwin.metadata;
//
//import com.ads.api.beans.common.Node;
//import com.erwin.sqlparser.ErwinSqlParserNat;
//import com.erwin.sqlparser.ErwinSqlparserNatOtherdb;
//import com.ads.api.beans.mm.Project;
//import com.ads.api.beans.mm.Subject;
//import com.ads.api.util.KeyValueUtil;
//import com.ads.api.util.MappingManagerUtil;
//import com.ads.api.util.SystemManagerUtil;
//import demos.visitors.toXml;
//import demos.visitors.xmlVisitor;
//import java.io.*;
//import java.util.Iterator;
//import java.util.LinkedHashSet;
//import java.util.Set;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//import org.apache.commons.io.FileUtils;
//import org.apache.commons.io.FilenameUtils;
//import demos.getstatement.getstatement;
//import gudusoft.gsqlparser.EDbVendor;
//import gudusoft.gsqlparser.TGSqlParser;
//import gudusoft.gsqlparser.pp.para.GFmtOpt;
//import gudusoft.gsqlparser.pp.para.GFmtOptFactory;
//import gudusoft.gsqlparser.pp.stmtformatter.FormatterFactory;
//import java.util.LinkedHashMap;
//import java.util.Map;
//import org.apache.commons.lang3.StringUtils;
//import com.erwin.metadata.Syncmetadata_Zovio_V2;
//import com.icc.util.RequestStatus;
//import java.util.List;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//import org.codehaus.jackson.map.ObjectMapper;
//import org.json.JSONArray;
//import org.json.JSONObject;
//
///**
// *
// * @author sashikant D
// */
//public class StroreProcedureParserWithoutWritingFolder {
//
//    public static void main(String[] args) throws Exception {
//      //  StroreProcedureParserWithoutWritingFolder.parseStoreprocintomultiplefile("E:\\Clients\\Harvard\\GetCustomerStoredProcedure\\test\\test", "sybase");
//        String query ="";
//    }
//    public static String parseStoreprocintomultiplefile(String storeProcFilePath, String dbVender,int projectid,MappingManagerUtil maputil,SystemManagerUtil smutil,KeyValueUtil keyValue,List<String> sysEnvDetails,String spsys,String spenv) throws Exception {
//        Set<String> storeproclist = null;
//        File storeProcFile = null;
//        File directory = null;
//        FileWriter writer = null;
//        FileOutputStream foutstream = null;
//        String inputFileName = "";
//        StringBuilder outputbuilder=new StringBuilder();
//        Pattern commentPattern = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);  //remove /* pattern in sql 
//        try {
//            storeProcFile = new File(storeProcFilePath);
//            if (storeProcFile.isDirectory()) {
//                File[] listofstoreprocFiles = storeProcFile.listFiles();
//                for (File listofstoreprocFile : listofstoreprocFiles) {
//                    int k = 1;
//                    String fileextension = FilenameUtils.getExtension(listofstoreprocFile.getName());
//                    inputFileName = listofstoreprocFile.getName();
//                    String query = FileUtils.readFileToString(listofstoreprocFile);
//                    query = commentPattern.matcher(query).replaceAll("");
//                  //  query = removeComments(query);
//                    storeproclist = getstatement.getallstatement(query, dbVender);
//                    if (storeproclist.size() == 1) {
//                        String sqlfile = FileUtils.readFileToString(listofstoreprocFile);
//                      //  sqlfile = WrapperForAddEnd.QueryFilterationAddingEnd(sqlfile);
//                        storeproclist = getstatement.getallstatementforstring(sqlfile, dbVender);
//                    }
//                    int i = 1;
//                    for (String storeproc : storeproclist) {
//                        storeproc = storeproc.toUpperCase();
//                        Set<String> selectInsertQueries = ErwinSqlParserNat.getAllStatementsForMSSQL(storeproc);
//
//                        for (String selectIntoQuerie : selectInsertQueries) {
//                            storeproc = selectIntoQuerie.toUpperCase();
//                            storeproc = storeproc.replaceAll("\\ + ", "\\ ").trim();
//                     if(!storeproc.toUpperCase().contains("@ERRMSG") && (!storeproc.toUpperCase().contains("@ERROR")))
//                        {
//                            inputFileName = listofstoreprocFile.getName();
//                            inputFileName = inputFileName + k;
//                            k++;
//                      String display=createMappingfromquery(storeproc,inputFileName,dbVender,projectid,maputil,smutil,keyValue,sysEnvDetails,spsys,spenv);
//                               // FileUtils.writeStringToFile(sqlfile, storeproc, "UTF-8");
//                              outputbuilder.append(display);
//                            }
//                        }
//                        }
//                    }
//                }
//        } catch (Exception e) {
//            StringWriter sw = new StringWriter();
//            e.printStackTrace(new PrintWriter(sw));
//            String exceptionAsString = sw.toString();
//            e.printStackTrace();
//        } finally {
//            if (writer != null) {
//                writer.close();
//            }
//
//        }
//        return outputbuilder.toString();
//    }
//
//    private static String getFileName(String storeProcedurFile) {
//        String inputFileName = "";
//        String[] procName = new String[0];
//        try {
//            //storeProcedurFile.toString();
//            String file[] = storeProcedurFile.split("\n");
//            String str = file[0];
//            if (str.contains("procedure")) {
//                procName = str.split("procedure");
//            } else if (str.contains("PROCEDURE")) {
//                procName = str.split("PROCEDURE");
//            } else if (str.contains("TABLE")) {
//                procName = str.split("TABLE");
//            } else if (str.contains("VIEW")) {
//                procName = str.split("VIEW");
//            } else if (str.contains("view")) {
//                procName = str.split("view");
//            } else if (str.contains("Procedure")) {
//                procName = str.split("Procedure");
//            } else if (str.contains("PROC")) {
//                procName = str.split("PROC");
//            } else if (str.contains("proc")) {
//                procName = str.split("proc");
//            }
//            if (procName.length > 1 && procName[1].contains("(")) {
//                if (procName[1].contains(".") && procName[1].split("\\.").length > 1) {
//                    procName[1] = procName[1].split("\\.")[1];
//                    String procedureName = procName[1].replace("(", "").replace(")", "").replace("[", "").replace("]", "");
//                    inputFileName = procedureName.replace("()", "").replace("`", "");
//                } else {
//                    String procedureName = procName[1].substring(0, procName[1].indexOf("("));
//                    inputFileName = procedureName.replace("()", "").replace("`", "");
//                }
//            } else if (procName.length > 1 && procName[1].contains("[")) {
//                if (procName[1].contains(".") && procName[1].split("\\.").length > 1) {
//                    procName[1] = procName[1].split("\\.")[1];
//                    String procedureName = procName[1].replace("[", "").replace("]", "").replace("(", "").replace(")", "");
//                    inputFileName = procedureName.replace("()", "").replace("`", "");
//                } else {
//                    String procedureName = procName[1].substring(0, procName[1].indexOf("("));
//                    inputFileName = procedureName.replace("()", "").replace("`", "");
//                }
//            }
//            if (inputFileName.contains("/")) {
//                inputFileName = inputFileName.replace("/", "");
//            }
//            if (inputFileName.contains(" ")) {
//                //str = str.replaceAll("\\s","")
//                inputFileName = inputFileName.replaceAll("\\s", "");
//            }
//            if (inputFileName.contains("*")) {
//                //str = str.replaceAll("\\s","")
//                inputFileName = inputFileName.replace("*", "");
//            }
//
//        } catch (Exception e) {
//
//        }
//        return inputFileName;
//    }
//
//    public static String createSubFolderInOutput(File query) {
//        Set<String> folderFilesset = new LinkedHashSet();
//        try {
//            //   File queryPath=new File(query);
//            String filename = query.getName();
//            String extension = FilenameUtils.getExtension(filename);
//            File queryFolder = new File(query.getParent() + File.separator + query.getName().replace(extension, ""));
//            if (!queryFolder.exists()) {
//                queryFolder.mkdir();
//            }
//            String sql = FileUtils.readFileToString(query);
//            folderFilesset = getStarSelectQuery(sql);
//            Iterator value = folderFilesset.iterator();
//            while (value.hasNext()) {
//                String file = value.next().toString();
//                FileUtils.writeStringToFile(queryFolder, file);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//   
//    public static Set<String> getStarSelectQuery(String query) {
//        Set<String> queryList = null;
//        try {
//            xmlVisitor.subselectquery.clear();
//            toXml.getdbvenderforsqltext(query);
//            queryList = new LinkedHashSet<>(xmlVisitor.subselectquery);
//
//            return queryList;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        return null;
//    }
//
//    /*Added by Narsimulu*/
//    public static Set<String> getUpdateQuery(String query) {
//        Set<String> queryList = null;
//        try {
//            xmlVisitor.updateQueryList.clear();
//            toXml.getdbvenderforsqltext(query);
//            queryList = new LinkedHashSet<>(xmlVisitor.updateQueryList);
//
//            return queryList;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        return null;
//    }
// public static String modifySelectQueries(String sql) {
//
//        String addedQuery = "INSERT INTO ";
//        String intoTable = "";
//        int spaceIndex = 0;
//        String returnQuery = "";
//        sql = sql.toUpperCase();
//        //  sql = getFileContent(sql);
//        //  sql = sql.replace("* \n", "*");
//        try {
//            if (sql.startsWith("(") && sql.endsWith(")")) {
//                sql = sql.substring(1, sql.length() - 1);
////                sql.equalsIgnoreCase(intoTable)
//            }
//            if (sql.contains("SELECT * INTO ")) {
//                String table = sql.split("\\* INTO ")[1];
//                spaceIndex = table.indexOf(" ");
//                if (spaceIndex != -1) {
//                    intoTable = table.substring(0, spaceIndex).trim();
//                } else {
//                    intoTable = table.substring(0, table.length());
//                }
//                if (intoTable.contains("\n")) {
//                    intoTable = intoTable.split("\n")[0].trim();
//                }
//                if (sql.startsWith("SELECT * INTO ")) {
//                    sql = sql.replace("SELECT * INTO " + intoTable, "SELECT *");
//                    returnQuery = addedQuery + " " + intoTable + "\n " + sql;
//
//                } else {
//                    String modifiedQuery = sql.split("\\* INTO ")[0];
//                    int selectStartLastIndex = modifiedQuery.lastIndexOf("SELECT");
//                    if (selectStartLastIndex != -1) {
//                        String insertQuery = modifiedQuery.substring(selectStartLastIndex, modifiedQuery.length()) + "* INTO " + sql.split("\\* INTO ")[1];
//                        insertQuery = insertQuery.replace("SELECT * INTO " + intoTable, "SELECT *");
//                        sql = sql.replace("SELECT * INTO " + intoTable, "SELECT *");
//                        String replaceQuery = addedQuery + " " + intoTable + "\n" + insertQuery;
//                        returnQuery = sql.replace(insertQuery, replaceQuery);
//                    } else {
//                        returnQuery = sql;
//                    }
//                }
//                returnQuery = formatterofSql(returnQuery);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return returnQuery;
//    }
//
//    public static String getFileContent(String sql) {
//        String fileContent = "";
//        try {
//            //  sql = sql.replaceAll("'--'", "'-'").replaceAll("'---'", "'-'").replaceAll("'----'", "'-'");
//            sql = sql.replaceAll("\r\n", "\n");
//            String[] lineArr = sql.split("\n");
//            String uncommentedLine = "";
//            boolean isCommentedLine = false;
//            for (String line : lineArr) {
//                if (isCommentedLine) {
//                    if (line.contains("*/")) {
//                        //if "*/" exists in the middle of the line, then extract right part excluding commented part
//                        if (uncommentedLine.length() > 0) {
//                            line = uncommentedLine + " " + line.substring(line.indexOf("*/") + 2);
//                        } else {
//                            line = line.substring(line.indexOf("*/") + 2);
//                        }
//                        if (line.contains("--")) {
//                            line = line.substring(0, line.indexOf("--"));
//                        }
//                        isCommentedLine = false;
//                        uncommentedLine = "";
//                    } else {
//                        continue;
//                    }
//                }
//                if (line.contains("--")) {
//                    if (line.trim().startsWith("--")) {
//                        line = "--";
//                    }
//                    //if "--" exists in the middle of the line, then extract left part excluding commented part
//                    if ((line.contains("/*") && line.indexOf("--") < line.indexOf("/*"))
//                            || (!line.contains("/*") && !line.contains("*/"))
//                            || (line.contains("/*") && line.contains("*/") && line.indexOf("--") > line.indexOf("*/"))) {
//                        line = line.substring(0, line.indexOf("--"));
//                    }
//                }
//                if (line.contains("/*") && line.contains("*/")) {
//                    line = line.substring(0, line.indexOf("/*")) + line.substring(line.indexOf("*/") + 2);
//                }
//                if (line.contains("/*") && !line.contains("*/")) {
//                    //if "/*" starts in the middle of the line, then extract left part excluding commented part
//                    uncommentedLine = line.substring(0, line.indexOf("/*"));
//                    isCommentedLine = true;
//                    continue;
//                }
//
//                if (line.trim().startsWith("--")) {
//                    line = "";
//                } else if (line.trim().toLowerCase().startsWith("print")) {
//                    line = "SET @PRINT='PRINT'";
//                }
//                line = line.replaceAll("\t", " ");
//                line = line.trim().replaceAll("( )+", " ");//replace multiple spaces with single space between words
//                line = line.replaceAll("' RENAME ", "'rename ").replaceAll("' Rename ", "'rename ").replaceAll("' rename ", "'rename ");
//
//                if (line.toLowerCase().contains("'rename ")) {
//                    line = line.replaceAll("'rename ", "'rename1 ");
//                }
//                if (line.length() == 0) {
//                    continue;
//                } else {
//                    fileContent += " " + line;
//                }
//            }
//            fileContent = fileContent.replaceAll("' \\(", "'\\(");
//            //fileContent = fileContent.replaceAll(" \\+", "+").replaceAll("\\+ ", "+");
//            fileContent = fileContent.replaceAll(" \\(", "(");
//            fileContent = fileContent.replaceAll("\t", " ");
//            fileContent = fileContent.replaceAll("=", " = ");
//            fileContent = fileContent.replaceAll(" REMOTE ", " ").replaceAll(" Remote ", " ").replaceAll(" remote ", " ");
//            fileContent = fileContent.replaceAll("' CREATE ", "'create ").replaceAll("' Create ", "'create ").replaceAll("' create ", "'create ");
//            fileContent = fileContent.replaceAll("' DELETE ", "'delete ").replaceAll("' Delete ", "'delete ").replaceAll("' delete ", "'delete ");
//            fileContent = fileContent.replaceAll("' INSERT ", "'insert ").replaceAll("' Insert ", "'insert ").replaceAll("' insert ", "'insert ");
//            fileContent = fileContent.replaceAll(" EXECUTE", " exec").replaceAll(" Execute", " exec").replaceAll(" execute", " exec");
//            fileContent = fileContent.replaceAll(" EXEC", " exec").replaceAll(" Exec", " exec");
//            fileContent = fileContent.replaceAll(" exec \\(", " exec\\(");
////            fileContent = fileContent.replaceAll(" SP_EXECUTESQL", " ").replaceAll(" SP_Executesql", " ").replaceAll(" SP_ExecuteSQL", " ").replaceAll(" SP_executesql", " ").replaceAll(" Sp_executesql", " ").replaceAll(" sp_executesql", " ");
//            fileContent = fileContent.replaceAll(" WHERECAST", " where cast").replaceAll(" WhereCast", " where cast").replaceAll(" wherecast", " where cast");
//            fileContent = fileContent.replaceAll("'\\( select ", "'\\(select ");
//            //fileContent = fileContent.replaceAll("' RENAME ", "'rename ").replaceAll("' Rename ", "'rename ").replaceAll("' rename ", "'rename ");
//            fileContent = fileContent.trim().replaceAll("( )+", " ");//replace multiple spaces with single space between words
//            fileContent = fileContent.replaceAll(" = N'", " = '").replaceAll(" = n'", " = '");
//            fileContent = fileContent.replaceAll(" \\+ = ", "+=");
//            fileContent = fileContent.replaceAll(" - = ", "-=");
//            if (fileContent.toUpperCase().startsWith("CREATE VIEW")) {
//                String createViewHead = fileContent.substring(0, fileContent.toUpperCase().indexOf(" AS "));
//                String modifiedViewHead = createViewHead.replaceAll("\\[", "").replaceAll("\\]", "");
//                fileContent = fileContent.replace(createViewHead, modifiedViewHead);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return fileContent;
//    }
//
//    public static String formatterofSql(String sqlStatement) {
//        try {
//            TGSqlParser gSqlParser = new TGSqlParser(EDbVendor.dbvmssql);
//            gSqlParser.setSqltext(sqlStatement);
//            int ret = gSqlParser.parse();
//            GFmtOpt option = GFmtOptFactory.newInstance();
//            sqlStatement = FormatterFactory.pp(gSqlParser, option);
//            //sqlStatement = removeUnwantedCharatersFromFromTable(sqlStatement);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return sqlStatement;
//    }
//   
//
//    public static String intosql(String sql) {
//        try {
//            int count = 0, i = 0;
//            String data = "INTO #AppInList";
//            File file = new File("C:\\Users\\VinithChalla\\Desktop\\New Text Document.txt");
//            File temp = new File("C:\\Users\\VinithChalla\\Desktop\\temp.txt");
//            if (temp.exists()) {
//                //           System.out.println("delete and create");
//                temp.delete();
//                temp.createNewFile();
//            } else {
//                //             System.out.println("create new file");
//                temp.createNewFile();
//            }
//            temp.createNewFile();
//            BufferedReader br = new BufferedReader(new FileReader(file));
//            PrintWriter pw = new PrintWriter(temp);
//            String st;
//            while ((st = br.readLine()) != null) {
//                if (st.trim().equalsIgnoreCase(data)) {
//                    count++;
//                } else {
//                    pw.println(st);
//                    pw.flush();
//                }
//            }
//            br.close();
//            pw.close();
//
//            //      System.out.println(count);
//            //      System.out.println(file.delete());
//            //     System.out.println(file.createNewFile());
//            PrintWriter pwr = new PrintWriter(file);
//            BufferedReader tempReader = new BufferedReader(new FileReader(temp));
//            while (i < count) {
//                pwr.println("INSERT " + data);
//                pwr.flush();
//                i++;
//            }
//            while ((st = tempReader.readLine()) != null) {
//                pwr.println(st);
//                pwr.flush();
//            }
//            pwr.close();
//            tempReader.close();
//            temp.delete();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return "";
//    }
//
//    public static String removecommentline(String query) {
//        StringBuilder sb = new StringBuilder();
//        try {
//            String[] quryline = query.split("\n");
//            for (String queryline : quryline) {
//                if (queryline.startsWith("--") || queryline.startsWith("---")) {
//                    continue;
//                }
//                sb.append(queryline).append("\n");
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        return sb.toString();
//    }
//
//    public static String removeComments(String data) {
//        try {
//            Pattern pattern = Pattern.compile("/\\*(.|\\n)*?\\*/");
//            Matcher matcher = pattern.matcher(data);
//            return matcher.replaceAll("");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return "";
//    }
//
//    private static String getUpdatedContent(String content) {
//        try {
//            content = content.replace("XMLELEMENT", "XMLELEMENT1").replace("XMLATTRIBUTES", "XMLATTRIBUTES1");
//            int startIndex = 0;
//            int formapstartindex2 = 0;
//            while (startIndex < content.length() && content.indexOf("XMLATTRIBUTES1", startIndex) != -1) {
//                startIndex = content.indexOf("XMLATTRIBUTES1", startIndex);
//                String data = content.substring(startIndex + "XMLATTRIBUTES1".length() + 1, content.indexOf(")", startIndex + "XMLATTRIBUTES1".length() + 1)).trim();
//                String[] attr = data.split(",");
//                StringBuffer out = new StringBuffer();
//                for (int i = 0; i < attr.length; i++) {
//                    String str = attr[i].trim().split("AS")[0].trim();
//                    if (i > 0) {
//                        out.append(",");
//                    }
//                    out.append(str);
//                }
//                content = content.substring(0, startIndex + "XMLATTRIBUTES1".length() + 1) + out.toString() + content.substring(content.indexOf(")", startIndex + "XMLATTRIBUTES1".length() + 1));
//                startIndex = content.indexOf(")", startIndex + "XMLATTRIBUTES1".length() + 1);
//
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return content;
//    }
//
//    private static String removeCommentLine(String sql) {
//        StringBuffer output = new StringBuffer();
//        String[] data = sql.split("\n");
//        for (String line : data) {
//            if (!line.trim().startsWith("--")) {
//                if (line.contains("--")) {
//                    line = line.substring(0, line.indexOf("--"));
//                }
//                output.append(line).append("\n");
//            }
//        }
//        return output.toString();
//    }
//
//    private static String removeNewLines(String data) {
//        StringBuilder output = new StringBuilder();
//        String[] lines = data.split("\n");
//        for (String line : lines) {
//            output.append(line.trim());
//        }
//        return output.toString();
//    }
//   public static String createMappingfromquery(String query,String mapName,String dbvendor,int projectId,MappingManagerUtil maputil,SystemManagerUtil smutil,KeyValueUtil keyValue,List<String> sysEnvDetails,String spsys,String spenv)
//   {
//       StringBuilder displayobj=new StringBuilder();
//     try
//     {
//        ErwinSqlparserNatOtherdb sqljsoncreator = new ErwinSqlparserNatOtherdb();
//         
//       //  mapObj = sqljsoncreator.sqlToDataflow(sqlfilearr[i].getAbsolutePath(),Sorcesystem, srcEnv, trgsytem, tgrtEnv, vDatabaseType,sqlfilearr[i].getName().replace(".sql",""));
//        String mapObj=ErwinSqlparserNatOtherdb.getJsonfromSqlstring(query, dbvendor, mapName,spsys,spenv);
//       
//         if (StringUtils.isNotBlank(mapObj)) 
//         {
//          Map keyValeuMap = new LinkedHashMap(sqljsoncreator.getKeyvalueJson());
//
//                JSONArray jsonArray=new JSONArray(mapObj);
//               
//                JSONObject jsonObject=jsonArray.getJSONObject(0);
//                mapName =jsonObject.getString("mappingName");
//                String subjectName = mapName;
//                subjectName = subjectName.replaceAll("[0-9]","");
//                subjectName = subjectName.replace(".","");
//        mapObj = Syncmetadata_Zovio_V2.metadataSync(sysEnvDetails,smutil,mapObj,subjectName);
//       int subjectId =createSubject(subjectName, projectId,maputil);
//                mapObj = mapObj.replaceAll("\"subjectId\":0","\"subjectId\":"+subjectId);
//                mapObj = mapObj.replaceAll("\"projectId\":0","\"projectId\":"+projectId);
//                  JSONArray  arr = new JSONArray(mapObj);
//                for (int k = 0; k < arr.length();k++) 
//                {
//                  JSONObject object = arr.getJSONObject(k);
//
//                    object.remove("childNodes");
//
//               String status = maputil.createMappingAs(object.toString(), "json");
//                    // var reqStatus = maputil.createMappingAs(mapObj,"json");
//
//             int mapId = maputil.getMappingId(subjectId,mapName,Node.NodeType.MM_SUBJECT);
//              String kstatus = keyValue.addKeyValueMap(keyValeuMap, "8", mapId +"").getStatusMessage();
//               //   RequestStatus rs=keyValue.addKeyValueMap(keyValeuMap,"8",mapId);
//             String gcStatus = status + "\n\n" + kstatus;
//             displayobj.append(mapName + "..." + gcStatus);
//                  //  gcStatus = gcStatus + "\n" + status + "\n\n"+ kstatus;
//
//                    sqljsoncreator.getClear();
//                }
//                   
//        }
//       }
//      
//     catch(Exception e)
//     {
//         e.printStackTrace();
//     }
//     return displayobj.toString();
//   }
//    public static int createSubject(String subjectName, int projectId, MappingManagerUtil maputil) {
//        int subjectId = 0;
//        try {
//            Project project = maputil.getProject(projectId);
//            String projectName = project.getProjectName();
//            subjectId = maputil.getSubjectId(projectName, subjectName);
//            if(subjectId!=0){
//               return subjectId;
//             }
//            if (subjectId == 0) {
//                // subjectId = mappingManagerUtil.getSubjectId(projectName, subjectName)
//                Subject subjectDetails = new Subject();
//
//                subjectDetails.setSubjectName(subjectName);
//                subjectDetails.setSubjectDescription("Oracle and sql Details");
//                subjectDetails.setProjectId(projectId);
//                RequestStatus retRS = maputil.createSubject(subjectDetails);
//                subjectId = maputil.getSubjectId(projectName, subjectName);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//     Logger.getLogger(StroreProcedureParserWithoutWritingFolder.class.getName()).log(Level.SEVERE, null, e);
//        }
//        return subjectId;
//    }
//
//}
//   
