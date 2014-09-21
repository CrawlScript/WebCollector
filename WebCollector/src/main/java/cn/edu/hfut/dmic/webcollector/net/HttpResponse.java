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

    private URL url;
    private int code;
    private Map<String,List<String>> headers=null;
    private byte[] content=null;
    
    public HttpResponse(URL url){
        this.url=url;
    }
    
    @Override
    public URL getUrl() {
        return url;
    }
    
    public void setUrl(URL url) {
        this.url = url;
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
    
    public void setContent(byte[] content) {
        this.content = content;
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

    @Override
    public String getContentType() {
        try{
        String contentType;
        List<String> contentTypeList = getHeader("Content-Type");
        if (contentTypeList == null) {
            contentType = null;
        } else {
            contentType = contentTypeList.get(0);
        }
            return contentType;
        }catch(Exception ex){
            return null;
        }
    }

    

    
    
    
    
    
    
}
