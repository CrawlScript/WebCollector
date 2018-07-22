///*
// * Copyright (C) 2014 hu
// *
// * This program is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//package cn.edu.hfut.dmic.webcollector.net;
//
//import cn.edu.hfut.dmic.webcollector.util.CharsetDetector;
//
//import java.net.URL;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import org.slf4j.LoggerFactory;
//
///**
// *
// * @author hu
// */
//@Deprecated
//public class HttpResponse {
//
//    public static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HttpResponse.class);
//
//    private URL url;
//    private int code;
//    private Map<String, List<String>> headers = null;
//    private byte[] content = null;
//    private boolean redirect = false;
//    private boolean notFound=false;
//    private String html = null;
//
//    private URL realUrl = null;
//
//    public HttpResponse(URL url) {
//        this.url = url;
//    }
//
//
//    public URL url() {
//        return url;
//    }
//
//    public void url(URL url) {
//        this.url = url;
//    }
//
//    @Deprecated
//    public URL getUrl() {
//        return url;
//    }
//
//    @Deprecated
//    public void setUrl(URL url) {
//        this.url = url;
//    }
//
//    /**
//     * 通过猜测编码的方式获取html源码字符串
//     * @return
//     */
//    public String decode() {
//        if (content == null) {
//            return null;
//        }
//        String charset = CharsetDetector.guessEncoding(content);
//        try {
//            String html = new String(content, charset);
//            return html;
//        } catch (Exception ex) {
//            LOG.info("Exception", ex);
//            return null;
//        }
//    }
//
//    public String decode(String charset) {
//        if (content == null) {
//            return null;
//        }
//        try {
//            String html = new String(content, charset);
//            return html;
//        } catch (Exception ex) {
//            LOG.info("Exception", ex);
//            return null;
//        }
//    }
//
//    @Deprecated
//    public String getHtml(String charset) {
//      return decode(charset);
//    }
//
//    @Deprecated
//    public String getHtmlByCharsetDetect() {
//        return decode();
//    }
//
//     public int code() {
//        return code;
//    }
//
//    public void code(int code) {
//        this.code = code;
//    }
//
//    @Deprecated
//    public int getCode() {
//        return code;
//    }
//    @Deprecated
//    public void setCode(int code) {
//        this.code = code;
//    }
//
//    public boolean isNotFound() {
//        return notFound;
//    }
//
//    public void setNotFound(boolean notFound) {
//        this.notFound = notFound;
//    }
//
//
//
//    public List<String> header(String name) {
//        if (headers == null) {
//            return null;
//        }
//        return headers.get(name);
//    }
//
//
//    @Deprecated
//    public List<String> getHeader(String name) {
//        return header(name);
//    }
//
//    public void headers(Map<String, List<String>> headers) {
//        this.headers = headers;
//    }
//
//    @Deprecated
//    public void setHeaders(Map<String, List<String>> headers) {
//        this.headers = headers;
//    }
//
//     public Map<String, List<String>> headers() {
//        return headers;
//    }
//    @Deprecated
//     public Map<String, List<String>> getHeaders() {
//        return headers;
//    }
//
//
//    public byte[] content() {
//        return content;
//    }
//
//    public void content(byte[] content) {
//        this.content = content;
//    }
//
//    @Deprecated
//    public byte[] getContent() {
//        return content;
//    }
//    @Deprecated
//    public void setContent(byte[] content) {
//        this.content = content;
//    }
//
//
//    public void setHeader(String key, List<String> values) {
//        if (headers == null) {
//            headers = new HashMap<String, List<String>>();
//        }
//        headers.put(key, values);
//    }
//
//    public void addHeader(String key, String value) {
//        if (headers == null) {
//            headers = new HashMap<String, List<String>>();
//            addHeader(key, value);
//        } else {
//            List<String> header = header(key);
//            if (header != null) {
//                header.add(value);
//            } else {
//                List<String> values = new ArrayList<String>();
//                values.add(value);
//                headers.put(key, values);
//            }
//        }
//
//    }
//
//
//    public String contentType() {
//        try {
//            String contentType;
//            List<String> contentTypeList = header("Content-Type");
//            if (contentTypeList == null) {
//                contentType = null;
//            } else {
//                contentType = contentTypeList.get(0);
//            }
//            return contentType;
//        } catch (Exception ex) {
//            LOG.info("Exception", ex);
//            return null;
//        }
//    }
//
//    @Deprecated
//    public String getContentType() {
//       return contentType();
//    }
//
//    public boolean isRedirect() {
//        return redirect;
//    }
//
//    public void setRedirect(boolean redirect) {
//        this.redirect = redirect;
//    }
//
//
//    public URL getRealUrl() {
//        if (realUrl == null) {
//            return url;
//        }
//        return realUrl;
//    }
//
//    public void setRealUrl(URL realUrl) {
//        this.realUrl = realUrl;
//    }
//
//    public String getHtml() {
//        return html;
//    }
//
//    public void setHtml(String html) {
//        this.html = html;
//    }
//
//}
