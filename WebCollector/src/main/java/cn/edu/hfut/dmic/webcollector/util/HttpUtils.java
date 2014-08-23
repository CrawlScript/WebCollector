/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import cn.edu.hfut.dmic.webcollector.model.Page;

/**
 *
 * @author hu
 */
public class HttpUtils {
    
   

    public static Page fetchHttpResponse(String url, int retry) {
        return fetchHttpResponse(url, null, retry);
    }

    public static Page fetchHttpResponse(String url) throws Exception {
        return fetchHttpResponse(url, null);
    }

    public static Page fetchHttpResponse(String url, ConnectionConfig conconfig, int retry) {
        for(int i=0;i<retry;i++){
            try{
                Page page=fetchHttpResponse(url, conconfig);
                return page;
            }catch(Exception ex){
                continue;
            }
        }
        return null;
 
    }

    public static Page fetchHttpResponseWithSize(String url, ConnectionConfig conconfig,int maxsize) throws Exception {
        URL _URL = new URL(url);
        HttpURLConnection con = (HttpURLConnection) _URL.openConnection();
        con.setDoInput(true);
        con.setDoOutput(true);
        if (conconfig != null) {
            conconfig.config(con);
        }
        InputStream is;
        if(con.getResponseCode()==403)
            is = con.getErrorStream();
        else
            is=con.getInputStream();
        byte[] buf = new byte[2048];
        int read;
        int sum=0;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while ((read = is.read(buf)) != -1) {
            if(maxsize>0){
            sum=sum+read;
                if(sum>maxsize){
                    read=maxsize-(sum-read);
                    bos.write(buf, 0, read);
                    Log.Infos("cut","cut size to "+maxsize);
                    break;
                }
            }
            bos.write(buf, 0, read);
        }

        is.close();
        Page page=new Page();
        page.content=bos.toByteArray();
        page.url=url;
        
        page.headers=con.getHeaderFields();
        return page;

    }
    
    public static Page fetchHttpResponse(String url, ConnectionConfig conconfig) throws Exception{
        return  fetchHttpResponseWithSize(url, conconfig, Config.maxsize);
    }

}
