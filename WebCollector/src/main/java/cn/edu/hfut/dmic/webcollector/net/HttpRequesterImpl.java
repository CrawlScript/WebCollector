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

import java.net.Proxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class HttpRequesterImpl implements HttpRequester {

    public static final Logger LOG = LoggerFactory.getLogger(HttpRequesterImpl.class);
    protected RequestConfig requestConfig = RequestConfig.createDefaultRequestConfig();;

    @Override
    public HttpResponse getResponse(String url) throws Exception {
        HttpRequest request = new HttpRequest(url, requestConfig);
        return request.getResponse();
    }

    public void setUserAgent(String userAgent) {
        requestConfig.setUserAgent(userAgent);
    }

    public void setCookie(String cookie) {
        requestConfig.setCookie(cookie);
    }

    public RequestConfig getRequestConfig() {
        return requestConfig;
    }

    public void setRequestConfig(RequestConfig requestConfig) {
        this.requestConfig = requestConfig;
    }

    public ProxyGenerator getProxyGenerator() {
        return requestConfig.getProxyGenerator();
    }

    public void setProxyGenerator(ProxyGenerator proxyGenerator) {
        requestConfig.setProxyGenerator(proxyGenerator);
    }

    public void setProxy(Proxy proxy) {
        requestConfig.setProxy(proxy);
        
    }

    public void setProxy(String host, int port, Proxy.Type type) {
        requestConfig.setProxy(host, port, type);
    }

    public void setProxy(String host, int port) {
        requestConfig.setProxy(host, port);
    }
    
     public String getMethod() {
        return requestConfig.getMethod();
    }

    public void setMethod(String METHOD) {
        requestConfig.setMethod(METHOD);
    }

    
     public void addHeader(String key, String value){
     requestConfig.addHeader(key, value);
    }
    
    public void removeHeader(String key){
        requestConfig.removeHeader(key);
    }

    public void setHeader(String key, String value) {
       requestConfig.setHeader(key, value);
    }
}
