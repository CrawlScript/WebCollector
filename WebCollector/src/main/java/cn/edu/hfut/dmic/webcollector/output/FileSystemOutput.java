/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.output;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import cn.edu.hfut.dmic.webcollector.util.Log;

/**
 *
 * @author hu
 */
public class FileSystemOutput {

    public String root;

    public FileSystemOutput(String root) {
        this.root = root;
    }

    public void output(Page page) {
        try {
            URL _URL = new URL(page.url);
            String query = "";
            if (_URL.getQuery() != null) {
                query = "_" + _URL.getQuery();
            }
            String path = _URL.getPath();
            if (path.length() == 0) {
                path = "index.html";
            } else {
                if (path.charAt(path.length() - 1) == '/') {
                    path = path + "index.html";
                } else {

                    for (int i = path.length() - 1; i >= 0; i--) {
                        if (path.charAt(i) == '/') {
                            if (!path.substring(i + 1).contains(".")) {
                                path = path + ".html";
                            }
                        }
                    }
                }
            }
            path += query;
            File domain_path = new File(root, _URL.getHost());
            File f = new File(domain_path, path);
            Log.Infos("output",null, f.getAbsolutePath());
            FileUtils.writeFileWithParent(f, page.content);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws MalformedURLException, IOException {

        URL _URL = new URL("http://www.zhihu.com/");
        System.out.println(_URL.getProtocol() + "://" + _URL.getHost());
        //System.out.println(f.getAbsolutePath());
        /*
         System.out.println(_URL.getProtocol());
         System.out.println(_URL.getPath());
        
         System.out.println(_URL.getHost());
         System.out.println(_URL.getQuery());
         */
    }

}
