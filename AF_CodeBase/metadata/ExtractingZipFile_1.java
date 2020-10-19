package com.erwin.metadata;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import static org.apache.commons.mail.ByteArrayDataSource.BUFFER_SIZE;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author InkolluReddy
 */
public class ExtractingZipFile_1 {

    public static ArrayList<String> paths = new ArrayList();
    public static Map<String, String> globalconnectionstringMap = new HashMap<String, String>();

    private static void extractEntry(final ZipEntry entry, InputStream is, String OUTPUT_DIR) throws IOException {
        String exractedFile = OUTPUT_DIR + entry.getName();
        if (exractedFile.endsWith("conmgr")) {
            paths.add(exractedFile);
        }
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(exractedFile);
            final byte[] buf = new byte[BUFFER_SIZE];
            int read = 0;
            int length;
            while ((length = is.read(buf, 0, buf.length)) >= 0) {
                fos.write(buf, 0, length);
            }
        } catch (IOException ioex) {
            fos.close();
        }
    }

    public static Map<String, String> zipfilepathextraction(String ispacfilepath) throws IOException {
        try {
            getclear();
            File sqlfiles = new File(ispacfilepath);
            if (sqlfiles.isDirectory()) {
                File listfiles[] = sqlfiles.listFiles();
                for (File listfile : listfiles) {
                    String zipDir = listfile.getAbsolutePath();
                    String zipFileName = listfile.getName();
                    if (zipFileName.endsWith(".ispac")) {
                        zipFileName = zipFileName.replace(".ispac", "");
                        zipFileName = zipFileName.trim();
                        final ZipFile ispacfile = new ZipFile(listfile);
                        try {
                            final Enumeration<? extends ZipEntry> entries = ispacfile.entries();
                            while (entries.hasMoreElements()) {
                                final ZipEntry entry = entries.nextElement();
                                System.out.printf("File..Name->" + entry.getName());
                                extractEntry(entry, ispacfile.getInputStream(entry), zipDir);
                            }

                            for (int i = 0; i < paths.size(); i++) {
                                String path = paths.get(i);
                                //return connection string Maps for getting
                                Map<String, String> localmap = gettingconnectionstringandobject(path);
                                for (Map.Entry<String, String> keyandvalues : localmap.entrySet()) {
                                    String key = keyandvalues.getKey();
                                    System.out.println("key" + "------>" + key);
                                    String value = keyandvalues.getValue();
                                    System.out.println("value" + "----->" + value);
                                    globalconnectionstringMap.put(key, value);
                                }
                            }

                        } finally {
                            ispacfile.close();
                        }

                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return globalconnectionstringMap;
    }

    public static Map<String, String> gettingconnectionstringandobject(String path) throws SAXException, IOException {
        Map<String, String> connectionMangermap = new HashMap();
        try {
            File file = new File(path);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(file);
            doc.getDocumentElement().normalize();
            Element rootelement = doc.getDocumentElement();
            //get all childNode
            String objectName = rootelement.getAttribute("DTS:ObjectName");
            NodeList childNodes = rootelement.getChildNodes();
            for (int i = 0; i < childNodes.getLength(); i++) {
//                System.out.println(childNodes.item(i).getNodeName());
                Node node = childNodes.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
//                    System.out.println(node.getNodeName());
                    //                    System.out.println(node.getNodeName());
                    if (node.getNodeName().equals("DTS:ObjectData")) {
                        NodeList objectDataNodeList = node.getChildNodes();
                        for (int j = 0; j < objectDataNodeList.getLength(); j++) {
                            Node node_1 = objectDataNodeList.item(j);
                            if (node_1.getNodeType() == Node.ELEMENT_NODE) {
                                if (node_1.getNodeName().equals("DTS:ConnectionManager")) {
                                    Element e = (Element) node_1;
                                    String conetionStringvalue = e.getAttribute("DTS:ConnectionString");
                                    connectionMangermap.put(objectName, conetionStringvalue);

                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connectionMangermap;
    }
    public static void xpath(String path)
    {
        try
        {
         File file = new File(path);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document xmlDocument = dBuilder.parse(file);
           XPath xPath = XPathFactory.newInstance().newXPath();
            String controlFlowLevelPath = "//DTS:Executable/DTS:LogProviders";
            NodeList controlFlowLevelNodeList = (NodeList) xPath.compile(controlFlowLevelPath).evaluate(xmlDocument, XPathConstants.NODESET);
             for (int i = 0; i < controlFlowLevelNodeList.getLength(); i++) {
                  NodeList rootChildNodeList = controlFlowLevelNodeList.item(i).getChildNodes();
                 for (int j = 0; j < rootChildNodeList.getLength(); j++) {
                if ("DTS:LogProvider".equalsIgnoreCase(rootChildNodeList.item((j)).getNodeName())) {
                    String  configstring = rootChildNodeList.item((j)).getAttributes().getNamedItem("DTS:ConfigString").getTextContent();
                    System.out.println(configstring);
             // String target = rootChildNodeList.item((j)).getAttributes().getNamedItem("DTS:To").getTextContent();
                }
                 }
             }
            controlFlowLevelNodeList.getLength();
            System.out.println(controlFlowLevelNodeList.getLength());
                   
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        
    }

    public static void main(String[] args) {
        try {
            zipfilepathextraction("E:\\Clients\\zovio\\Zoviopackages\\ispac");
            //   gettingconnectionstringandobject("E:\\Clients\\zovio\\Zoviopackages\\ispac\\Lead.ispacconn_OLEDB_BPI_DW_STAGE.conmgr");
          // xpath("E:\\Clients\\zovio\\Zoviopackages\\ispac\\Lead.ispacDim_Main_Lead.dtsx");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void getclear() {
        paths.clear();
        globalconnectionstringMap.clear();
    }
}
