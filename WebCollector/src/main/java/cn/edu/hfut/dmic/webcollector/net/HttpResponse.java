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


import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Response的一种实现，WebCollector默认使用HttpResponse作为http响应
 * @author hu
 */
public class HttpResponse{

    private String url;
    private int code;
    private Map<String,List<String>> headers=null;
    private byte[] content=null;
    
    public HttpResponse(String url){
        this.url=url;
    }
    
    
    public String getUrl() {
        return url;
    }
    
    public void setUrl(String url) {
        this.url = url;
    }

   
    public int getCode() {
        return code;
    }

    
    public List<String> getHeader(String name) {
        return headers.get(name);
    }

    
    public byte[] getContent() {
        return content;
    }
    
    public void setContent(byte[] content) {
        this.content = content;
    }

    public void setCode(int code) {
        this.code = code;
    }

    
    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    
    public void setHeaders(Map<String, List<String>> headers) {
        this.headers=headers;
    }

    
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
