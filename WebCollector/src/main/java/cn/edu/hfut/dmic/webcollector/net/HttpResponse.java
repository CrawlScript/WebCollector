/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.net;

import com.sun.net.httpserver.Headers;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 *
 * @author hu
 */
public class HttpResponse implements Response{

    public URL url;
    public int code;
    public Map<String,List<String>> headers=null;
    public byte[] content=null;
    
    public HttpResponse(URL url){
        this.url=url;
    }
    
    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public int getCode() {
        return code;
    }

    @Override
    public List<String> getHeader(String name) {
        return headers.get(name);
    }

    @Override
    public byte[] getContent() {
        return content;
    }

    public void setCode(int code) {
        this.code = code;
    }

    @Override
    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    @Override
    public void setHeaders(Map<String, List<String>> headers) {
        this.headers=headers;
    }
    
    
    
}
