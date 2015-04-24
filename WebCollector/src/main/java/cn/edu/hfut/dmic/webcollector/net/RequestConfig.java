/*
 * Copyright (C) 2015 hu
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

import cn.edu.hfut.dmic.webcollector.util.Config;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 * @author hu
 */
public class RequestConfig {

    protected ProxyGenerator proxyGenerator = null;

    protected int MAX_REDIRECT = Config.MAX_REDIRECT;
    protected int MAX_RECEIVE_SIZE = Config.MAX_RECEIVE_SIZE;
    protected String METHOD = "GET";
    protected boolean doinput = true;
    protected boolean dooutput = true;
    protected boolean followRedirects = false;
    protected int timeoutForConnect = Config.TIMEOUT_CONNECT;
    protected int timeoutForRead = Config.TIMEOUT_READ;


    public static RequestConfig createDefaultRequestConfig() {
        RequestConfig requestConfig = new RequestConfig();
        requestConfig.setUserAgent(Config.DEFAULT_USER_AGENT);
        return requestConfig;
    }

    public static RequestConfig createDefaultRequestConfig(String userAgent, String cookie) {
        RequestConfig requestConfig = new RequestConfig();
        if (userAgent != null) {
            requestConfig.setUserAgent(userAgent);
        }
        if (cookie != null) {
            requestConfig.setCookie(cookie);
        }
        return requestConfig;
    }
    
    public static RequestConfig createDefaultRequestConfig(String userAgent) {
        RequestConfig requestConfig = new RequestConfig();
        if (userAgent != null) {
            requestConfig.setUserAgent(userAgent);
        }
        return requestConfig;
    }

    public void config(HttpURLConnection con) throws Exception {

        con.setRequestMethod(METHOD);

        con.setInstanceFollowRedirects(followRedirects);

        con.setDoInput(doinput);
        con.setDoOutput(dooutput);

        con.setConnectTimeout(timeoutForConnect);
        con.setReadTimeout(timeoutForRead);

        if (headerMap != null) {
            for (Entry<String, List<String>> entry : headerMap.entrySet()) {
                String key = entry.getKey();
                List<String> valueList = entry.getValue();
                for (String value : valueList) {
                    con.addRequestProperty(key, value);
                }
            }
        }
    }

    protected Map<String, List<String>> headerMap = null;

    private void initHeaderMap() {
        if (headerMap == null) {
            headerMap = new HashMap<String, List<String>>();
        }
    }

    public void setUserAgent(String userAgent) {
        setHeader("User-Agent", userAgent);
    }

    public void setCookie(String cookie) {
        setHeader("Cookie", cookie);
    }

    public void addHeader(String key, String value){
        if(key==null){
            throw new NullPointerException("key is null");
        }
        if(value==null){
            throw new NullPointerException("value is null");
        }
        initHeaderMap();
        List<String> valueList = headerMap.get(key);
        if (valueList == null) {
            valueList = new ArrayList<String>();
            headerMap.put(key, valueList);
        }
        valueList.add(value);
    }
    
    public void removeHeader(String key){
        if(key==null){
            throw new NullPointerException("key is null");
        }
       
        if(headerMap!=null){
            headerMap.remove(key);
        }
    }

    public void setHeader(String key, String value) {
        if(key==null){
            throw new NullPointerException("key is null");
        }
        if(value==null){
            throw new NullPointerException("value is null");
        }
        initHeaderMap();
        List<String> valueList = new ArrayList<String>();
        valueList.add(value);
        headerMap.put(key, valueList);
    }

    public int getMAX_REDIRECT() {
        return MAX_REDIRECT;
    }

    public void setMAX_REDIRECT(int MAX_REDIRECT) {
        this.MAX_REDIRECT = MAX_REDIRECT;
    }

    public int getMAX_RECEIVE_SIZE() {
        return MAX_RECEIVE_SIZE;
    }

    public void setMAX_RECEIVE_SIZE(int MAX_RECEIVE_SIZE) {
        this.MAX_RECEIVE_SIZE = MAX_RECEIVE_SIZE;
    }

  

    public String getMethod() {
        return METHOD;
    }

    public void setMethod(String METHOD) {
        this.METHOD = METHOD;
    }

    public Map<String, List<String>> getHeaders(){
        return headerMap;
    }
    
    public List<String> getHeader(String key){
        if(headerMap==null){
            return null;
        }
        return headerMap.get(key);
    }
    
    public String getFirstHeader(String key){
         if(headerMap==null){
            return null;
        }
         List<String> valueList=headerMap.get(key);
         if(valueList.size()>0){
             return valueList.get(0);
         }else{
             return null;
         }
    }
    
    
    public boolean isDoinput() {
        return doinput;
    }

    public void setDoinput(boolean doinput) {
        this.doinput = doinput;
    }

    public boolean isDooutput() {
        return dooutput;
    }

    public void setDooutput(boolean dooutput) {
        this.dooutput = dooutput;
    }

    public int getTimeoutForConnect() {
        return timeoutForConnect;
    }

    public void setTimeoutForConnect(int timeoutForConnect) {
        this.timeoutForConnect = timeoutForConnect;
    }

    public int getTimeoutForRead() {
        return timeoutForRead;
    }

    public void setTimeoutForRead(int timeoutForRead) {
        this.timeoutForRead = timeoutForRead;
    }

    public ProxyGenerator getProxyGenerator() {
        return proxyGenerator;
    }

    public void setProxyGenerator(ProxyGenerator proxyGenerator) {
        this.proxyGenerator = proxyGenerator;
    }

    public void setProxy(Proxy proxy) {
        this.proxyGenerator = new SingleProxyGenerator(proxy);
    }

    public void setProxy(String host, int port, Proxy.Type type) {
        this.proxyGenerator = new SingleProxyGenerator(host, port, type);
    }

    public void setProxy(String host, int port) {
        this.proxyGenerator = new SingleProxyGenerator(host, port);
    }

    
   

    
}
