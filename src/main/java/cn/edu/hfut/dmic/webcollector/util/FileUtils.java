/*
 * Copyright (C) 2014 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package cn.edu.hfut.dmic.webcollector.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 *
 * @author hu
 */
public class FileUtils {

    public static void deleteDir(File dir) {
        File[] filelist = dir.listFiles();
        for (File file : filelist) {
            if (file.isFile()) {
                file.delete();
            } else {
                deleteDir(file);
            }
        }
        dir.delete();
    }

    public static void copy(File origin, File newfile) throws FileNotFoundException, IOException {
        if (!newfile.getParentFile().exists()) {
            newfile.getParentFile().mkdirs();
        }
        FileInputStream fis = new FileInputStream(origin);
        FileOutputStream fos = new FileOutputStream(newfile);
        byte[] buf = new byte[2048];
        int read;
        while ((read = fis.read(buf)) != -1) {
            fos.write(buf, 0, read);
        }
        fis.close();
        fos.close();
    }

    public static void write(String fileName, String contentStr, String charset) throws FileNotFoundException, IOException {
        File file = new File(fileName);
        File parent = file.getParentFile();
        if (!parent.exists()) {
            parent.mkdirs();
        }
        byte[] content = contentStr.getBytes(charset);
        FileOutputStream fos = new FileOutputStream(file);
        fos.write(content);
        fos.close();
    }

    public static void write(File file, String contentStr, String charset) throws FileNotFoundException, IOException {
        File parent = file.getParentFile();
        if (!parent.exists()) {
            parent.mkdirs();
        }
        byte[] content = contentStr.getBytes(charset);
        FileOutputStream fos = new FileOutputStream(file);
        fos.write(content);
        fos.close();
    }
    
    
    public static void write(String fileName, byte[] content) throws FileNotFoundException, IOException {
        File file = new File(fileName);
        File parent = file.getParentFile();
        if (!parent.exists()) {
            parent.mkdirs();
        }
        FileOutputStream fos = new FileOutputStream(file);
        fos.write(content);
        fos.close();
    }


    public static void write(File file, byte[] content) throws FileNotFoundException, IOException {

        File parent = file.getParentFile();
        if (!parent.exists()) {
            parent.mkdirs();
        }
        FileOutputStream fos = new FileOutputStream(file);
        fos.write(content);
        fos.close();
    }


    
     public static byte[] read(File file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        byte[] buf = new byte[2048];
        int read;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while ((read = fis.read(buf)) != -1) {
            bos.write(buf, 0, read);
        }

        fis.close();
        return bos.toByteArray();
    }

    public static byte[] read(String fileName) throws IOException {
        File file = new File(fileName);
        return read(file);
    }

    public static String read(File file, String charset) throws Exception {
        FileInputStream fis = new FileInputStream(file);
        byte[] buf = new byte[2048];
        int read;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while ((read = fis.read(buf)) != -1) {
            bos.write(buf, 0, read);
        }

        fis.close();
        return new String(bos.toByteArray(), charset);
    }

    public static String read(String fileName, String charset) throws Exception {
        File file = new File(fileName);
        return read(file, charset);
    }
    


}
