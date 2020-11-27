package com.erwin.metadata_zovio_af;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.FileUtils;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author InkolluReddy
 */
public class ExtractingZipFile {
    
public static void unzip(String zipFilePath, String destDir) {
        FileInputStream fis;
        byte[] buffer = new byte[1024];
        try {
                fis = new FileInputStream(zipFilePath);
            try (ZipInputStream zis = new ZipInputStream(fis)) {
                ZipEntry ze = zis.getNextEntry();
                while (ze != null) {
                    String fileName = ze.getName();
                    File newFile = new File(destDir + File.separator + fileName);
                    new File(newFile.getParent()).mkdirs();
                    try (FileOutputStream fos = new FileOutputStream(newFile)) {
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                    zis.closeEntry();
                    ze = zis.getNextEntry();
                }
                zis.closeEntry();
            }
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[]args)
    {
        File sqlfiles = new File("E:\\Clients\\zovio\\Zoviopackages\\ispac\\Lead.ispac");  
        if(sqlfiles.isDirectory())
        {
         File listfiles[]=sqlfiles.listFiles();
        }
        String zipDir=sqlfiles.getParent();
        String zipFileName=sqlfiles.getName();
        if(zipFileName.endsWith(".ispac")) {
                zipFileName = zipFileName.replace(".ispac", "");
                zipFileName = zipFileName.trim();
                zipDir = zipDir + "/" + zipFileName + "/";
                File targetDir = new File(zipDir);
                if (!targetDir.isDirectory()) {
                    targetDir.mkdir();
                }
            //   FileUtils.copyFile(sourceFile, new File(targetDir + "/" + zipFileName + ".zip"));
                String targetZipFile = targetDir + "/" + zipFileName + ".zip";              
                unzip(targetZipFile, zipDir);
                File[] tfiles = targetDir.listFiles();
                for (File tfile : tfiles) {
                    if(tfile.getName().endsWith(".dtsx")) {
                        System.out.println(tfile.getName());
                    }
                }
            }
        
        
        
    }
    
}
