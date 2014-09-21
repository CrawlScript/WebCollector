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

package cn.edu.hfut.dmic.webcollector.net;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;


/**
 * Request的一种实现。WebCollector默认使用HttpRequest作为http请求
 * @author hu
 */
public class HttpRequest implements Request{

    private URL url=null;
    private Proxy proxy=null;
    private ConnectionConfig config=null;

    @Override
    public URL getURL() {
        return url;
    }

    @Override
    public void setURL(URL url) {
        this.url=url;
    }

    @Override
    public Response getResponse(CrawlDatum datum) throws Exception {  
        HttpResponse response=new HttpResponse(url);
        HttpURLConnection con;
        
        if(proxy==null){
            con=(HttpURLConnection) url.openConnection();
        }else{
            con=(HttpURLConnection) url.openConnection(proxy);
        }

        con.setDoInput(true);
        con.setDoOutput(true);
        
        if(config!=null){
            config.config(con);
        }
        
        
        InputStream is;
  
        response.setCode(con.getResponseCode());
        
        
        is=con.getInputStream();
            

        byte[] buf = new byte[2048];
        int read;
        int sum=0;
        int maxsize=Config.maxsize;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while ((read = is.read(buf)) != -1) {
            if(maxsize>0){
            sum=sum+read;
                if(sum>maxsize){
                    read=maxsize-(sum-read);
                    bos.write(buf, 0, read);                    
                    break;
                }
            }
            bos.write(buf, 0, read);
        }

        is.close();       
        
        response.setContent(bos.toByteArray());
        response.setHeaders(con.getHeaderFields());
        return response;
    }
    

   
    /**
     * 设置代理
     * @param proxy 代理
     */
    public void setProxy(Proxy proxy) {
        this.proxy=proxy;
    }

    /**
     * 返回代理
     * @return 代理
     */
    public Proxy getProxy() {
        return proxy;
    }

    /**
     * 设置http连接配置对象
     * @param config http连接配置对象
     */
    public void setConnectionConfig(ConnectionConfig config) {
        this.config=config;
    }
    
    /**
     * 返回http连接配置对象
     * @return
     */
    public ConnectionConfig getConconfig() {
        return config;
    }
    
    
}
