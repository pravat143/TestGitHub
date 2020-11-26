/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.metadata_zovio_af;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 *
 @author InkolluReddy
 */
public class ParsingTextFile {
    public static Map<String, String> readTextFile(String filepath, String sysEnvname) {
        Map<String, String> textFileInfo = new LinkedHashMap<>();
        Path path = null;
        Scanner sc = null;
        String fileContent = null;
        String fileContentArray[] = null;
        int count = 0;
        LinkedHashMap<String, Integer> columnIndices = new LinkedHashMap();
        String columnHeader = "";
        Integer columnIndex = null;
        String systemName = "";
        String environment = "";
        String table = "";
        String database = "";
        List<String> list = null;
        String sysEnvnameArr[] = null;
        try {
          
             if (sysEnvname.contains("###")) {
                sysEnvnameArr = sysEnvname.split("###");
                list = Arrays.asList(sysEnvnameArr);
            }else{
                list=new ArrayList<>();
                list.add(sysEnvname);
            }
           // sysEnvnameArr = sysEnvname.split("###");
           // list = Arrays.asList(sysEnvnameArr);
            //create path object representing filePath
            path = Paths.get(filepath);
            //create Scanner class object
            sc = new Scanner(path);
            //read the File line by line
            while (sc.hasNextLine()) {
                systemName = "";
                environment = "";
                table = "";
                database="";
                fileContent = sc.nextLine();
                //split the fileContent by using delimeter ','
                fileContentArray = fileContent.split(",");
                if (count == 0) {
                    for (int i = 0; i < fileContentArray.length; i++) {
                        columnIndices.put(fileContentArray[i].trim(), i);
                    }
                    count++;
                }//if
                else {
                    for (Map.Entry<String, Integer> entry : columnIndices.entrySet()) {
                        columnHeader = entry.getKey();
                        columnIndex = entry.getValue();
                        if (columnHeader.equalsIgnoreCase("SystemName")) {
                            if (columnIndex < fileContentArray.length) {
                                systemName = fileContentArray[columnIndex];
                            }
                        }//if
                        else if (columnHeader.equalsIgnoreCase("EnvName")) {
                            if (columnIndex < fileContentArray.length) {
                                environment = fileContentArray[columnIndex];
                            }
                        }//else if
                        else if (columnHeader.equalsIgnoreCase("TableName")) {
                            if (columnIndex < fileContentArray.length) {
                                table = fileContentArray[columnIndex];
                            }
                        }
                        else if (columnHeader.equalsIgnoreCase("DatabaseName")) {
                            if (columnIndex < fileContentArray.length) {
                                database = fileContentArray[columnIndex];
                            }
                        }
                    }//for
                    //trim all the values befor keeping in Map
                    systemName = systemName.trim();
                    environment = environment.trim();
                    table = table.toUpperCase();
                    table = table.trim(); 
                    database = database.toUpperCase().trim();
                    //keep in hashmap if if matches the condition
                    if (systemName != "" && environment != "" && table != "") {
                        if (list.contains(systemName.toUpperCase() + "," + environment.toUpperCase())) {
                             textFileInfo.put(table, environment + "#" + systemName);
                             textFileInfo.put(database, environment + "#" + systemName);
                            

                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return textFileInfo;
    }

//    public static void main(String[] args) {
//        String val = "Sales Data Mart,BI_ODS_Ektron#erwin#MDM$MasterData,tbsql47w\\maestro";
//        readTextFile("D:\\SQL Parser\\metadatafile\\Metadata.txt", val);
//        System.out.println(textFileInfo);
//    }
}
