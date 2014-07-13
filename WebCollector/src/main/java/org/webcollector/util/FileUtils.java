/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.webcollector.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 *
 * @author hu
 */
public class FileUtils {
    
    public static void writeFile(String filename,byte[] content) throws FileNotFoundException, IOException{
        FileOutputStream fos=new FileOutputStream(filename);
        fos.write(content);
        fos.close();
    }
    
    public static void writeFileWithParent(String filename,byte[] content) throws FileNotFoundException, IOException{
        File file=new File(filename);
        File parent=file.getParentFile();
        if(!parent.exists()){
            parent.mkdirs();
        }
        FileOutputStream fos=new FileOutputStream(file);
        fos.write(content);
        fos.close();
    }
    
    public static void writeFileWithParent(File file,byte[] content) throws FileNotFoundException, IOException{
       
        File parent=file.getParentFile();
        if(!parent.exists()){
            parent.mkdirs();
        }
        FileOutputStream fos=new FileOutputStream(file);
        fos.write(content);
        fos.close();
    }
    
}
