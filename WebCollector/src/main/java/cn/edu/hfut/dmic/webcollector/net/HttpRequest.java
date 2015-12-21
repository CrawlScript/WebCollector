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

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.Config;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class HttpRequest {

    public static final Logger LOG = LoggerFactory.getLogger(HttpRequest.class);

    protected int MAX_REDIRECT = Config.MAX_REDIRECT;
    protected int MAX_RECEIVE_SIZE = Config.MAX_RECEIVE_SIZE;
    protected String method = Config.DEFAULT_HTTP_METHOD;
    protected boolean doinput = true;
    protected boolean dooutput = true;
    protected boolean followRedirects = false;
    protected int timeoutForConnect = Config.TIMEOUT_CONNECT;
    protected int timeoutForRead = Config.TIMEOUT_READ;
    protected byte[] outputData=null;
    Proxy proxy = null;

    protected Map<String, List<String>> headerMap = null;

    protected CrawlDatum crawlDatum = null;

    public HttpRequest(String url) throws Exception {
        this.crawlDatum = new CrawlDatum(url);
        setUserAgent(Config.DEFAULT_USER_AGENT);
    }

    public HttpRequest(String url, Proxy proxy) throws Exception {
        this(url);
        this.proxy = proxy;
    }

    public HttpRequest(CrawlDatum crawlDatum) throws Exception {
        this.crawlDatum = crawlDatum;
        setUserAgent(Config.DEFAULT_USER_AGENT);
    }

    public HttpRequest(CrawlDatum crawlDatum, Proxy proxy) throws Exception {
        this(crawlDatum);
        this.proxy = proxy;
    }

    public HttpResponse getResponse() throws Exception {
        URL url = new URL(crawlDatum.getUrl());
        HttpResponse response = new HttpResponse(url);
        int code = -1;
        int maxRedirect = Math.max(0, MAX_REDIRECT);
        HttpURLConnection con = null;
        InputStream is = null;
        try {

            for (int redirect = 0; redirect <= maxRedirect; redirect++) {
                if (proxy == null) {
                    con = (HttpURLConnection) url.openConnection();
                } else {
                    con = (HttpURLConnection) url.openConnection(proxy);
                }

                config(con);
                
                if(outputData!=null){
                    OutputStream os=con.getOutputStream();
                    os.write(outputData);
                    os.close();
                }

                code = con.getResponseCode();
                /*只记录第一次返回的code*/
                if (redirect == 0) {
                    response.setCode(code);
                }
                
                if(code==HttpURLConnection.HTTP_NOT_FOUND){
                    response.setNotFound(true);
                    return response;
                }

                boolean needBreak = false;
                switch (code) {
                        
                    case HttpURLConnection.HTTP_MOVED_PERM:
                    case HttpURLConnection.HTTP_MOVED_TEMP:
                        response.setRedirect(true);
                        if (redirect == MAX_REDIRECT) {
                            throw new Exception("redirect to much time");
                        }
                        String location = con.getHeaderField("Location");
                        if (location == null) {
                            throw new Exception("redirect with no location");
                        }
                        String originUrl = url.toString();
                        url = new URL(url, location);
                        response.setRealUrl(url);
                        LOG.info("redirect from " + originUrl + " to " + url.toString());
                        continue;
                    default:
                        needBreak = true;
                        break;
                }
                if (needBreak) {
                    break;
                }

            }

            is = con.getInputStream();
            String contentEncoding = con.getContentEncoding();
            if (contentEncoding != null && contentEncoding.equals("gzip")) {
                is = new GZIPInputStream(is);
            }

            byte[] buf = new byte[2048];
            int read;
            int sum = 0;
            int maxsize = MAX_RECEIVE_SIZE;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            while ((read = is.read(buf)) != -1) {
                if (maxsize > 0) {
                    sum = sum + read;

                    if (maxsize > 0 && sum > maxsize) {
                        read = maxsize - (sum - read);
                        bos.write(buf, 0, read);
                        break;
                    }
                }
                bos.write(buf, 0, read);
            }

            response.setContent(bos.toByteArray());
            response.setHeaders(con.getHeaderFields());
            bos.close();

            return response;
        } catch (Exception ex) {
            throw ex;
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    public void config(HttpURLConnection con) throws Exception {

        con.setRequestMethod(method);

        con.setInstanceFollowRedirects(followRedirects);

        con.setDoInput(doinput);
        con.setDoOutput(dooutput);

        con.setConnectTimeout(timeoutForConnect);
        con.setReadTimeout(timeoutForRead);

        if (headerMap != null) {
            for (Map.Entry<String, List<String>> entry : headerMap.entrySet()) {
                String key = entry.getKey();
                List<String> valueList = entry.getValue();
                for (String value : valueList) {
                    con.addRequestProperty(key, value);
                }
            }
        }
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method=method;
    }

    public CrawlDatum getCrawlDatum() {
        return crawlDatum;
    }

    public void setCrawlDatum(CrawlDatum crawlDatum) {
        this.crawlDatum = crawlDatum;
    }

    static {
        TrustManager[] trustAllCerts = new TrustManager[]{
            new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(
                        java.security.cert.X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(
                        java.security.cert.X509Certificate[] certs, String authType) {
                }
            }
        };

        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (Exception ex) {
            LOG.info("Exception", ex);
        }
    }

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

    public void addHeader(String key, String value) {
        if (key == null) {
            throw new NullPointerException("key is null");
        }
        if (value == null) {
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

    public void removeHeader(String key) {
        if (key == null) {
            throw new NullPointerException("key is null");
        }

        if (headerMap != null) {
            headerMap.remove(key);
        }
    }

    public void setHeader(String key, String value) {
        if (key == null) {
            throw new NullPointerException("key is null");
        }
        if (value == null) {
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

   

    public Map<String, List<String>> getHeaders() {
        return headerMap;
    }

    public List<String> getHeader(String key) {
        if (headerMap == null) {
            return null;
        }
        return headerMap.get(key);
    }

    public String getFirstHeader(String key) {
        if (headerMap == null) {
            return null;
        }
        List<String> valueList = headerMap.get(key);
        if (valueList.size() > 0) {
            return valueList.get(0);
        } else {
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

    public Proxy getProxy() {
        return proxy;
    }

    public void setProxy(Proxy proxy) {
        this.proxy = proxy;
    }

    public Map<String, List<String>> getHeaderMap() {
        return headerMap;
    }

    public void setHeaderMap(Map<String, List<String>> headerMap) {
        this.headerMap = headerMap;
    }

    public boolean isFollowRedirects() {
        return followRedirects;
    }

    public void setFollowRedirects(boolean followRedirects) {
        this.followRedirects = followRedirects;
    }

    public byte[] getOutputData() {
        return outputData;
    }

    public void setOutputData(byte[] outputData) {
        this.outputData = outputData;
        this.dooutput=true;
    }
    
    

}
