/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.webcollector.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import org.webcollector.model.Page;

/**
 *
 * @author hu
 */
public class HttpUtils {
    
    

    public static Page fetchHttpResponse(String url, int count) {
        return fetchHttpResponse(url, null, count);
    }

    public static Page fetchHttpResponse(String url) throws Exception {
        return fetchHttpResponse(url, null);
    }

    public static Page fetchHttpResponse(String url, ConnectionConfig conconfig, int count) {
        HttpRetry httpretry = new HttpRetry(url, conconfig);
        return httpretry.getResult(count);
    }

    public static Page fetchHttpResponse(String url, ConnectionConfig conconfig) throws Exception {
        URL _URL = new URL(url);
        HttpURLConnection con = (HttpURLConnection) _URL.openConnection();
        con.setDoInput(true);
        con.setDoOutput(true);
        if (conconfig != null) {
            conconfig.config(con);
        }
        InputStream is = con.getInputStream();
        byte[] buf = new byte[2048];
        int read;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while ((read = is.read(buf)) != -1) {
            bos.write(buf, 0, read);
        }

        is.close();
        Page page=new Page();
        page.content=bos.toByteArray();
        page.url=url;
        
        page.headers=con.getHeaderFields();
        return page;

    }

}
