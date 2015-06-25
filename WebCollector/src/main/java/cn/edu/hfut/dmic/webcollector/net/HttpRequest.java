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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
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

    protected URL url;
    protected RequestConfig requestConfig = null;

    public HttpRequest(String url) throws Exception {
        this.url = new URL(url);
        requestConfig = RequestConfig.createDefaultRequestConfig();
    }

    public HttpRequest(String url, RequestConfig requestConfig) throws Exception {
        this.url = new URL(url);
        this.requestConfig = requestConfig;
    }

    public HttpResponse getResponse() throws Exception {
        HttpResponse response = new HttpResponse(url);
        int code = -1;
        int maxRedirect = Math.max(0, requestConfig.getMAX_REDIRECT());
        Proxy proxy;
        ProxyGenerator proxyGenerator = requestConfig.getProxyGenerator();
        if (proxyGenerator == null) {
            proxy = null;
        } else {
            proxy = proxyGenerator.next(url.toString());
        }

        try {
            HttpURLConnection con = null;
            for (int redirect = 0; redirect <= maxRedirect; redirect++) {
                if (proxy == null) {
                    con = (HttpURLConnection) url.openConnection();
                } else {
                    con = (HttpURLConnection) url.openConnection(proxy);
                }

                requestConfig.config(con);

                code = con.getResponseCode();
                /*只记录第一次返回的code*/
                if (redirect == 0) {
                    response.setCode(code);
                }

                boolean needBreak = false;
                switch (code) {
                    case HttpURLConnection.HTTP_MOVED_PERM:
                    case HttpURLConnection.HTTP_MOVED_TEMP:
                        response.setRedirect(true);
                        if (redirect == requestConfig.MAX_REDIRECT) {
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

            InputStream is;

            is = con.getInputStream();
            String contentEncoding = con.getContentEncoding();
            if (contentEncoding != null && contentEncoding.equals("gzip")) {
                is = new GZIPInputStream(is);
            }

            byte[] buf = new byte[2048];
            int read;
            int sum = 0;
            int maxsize = requestConfig.getMAX_RECEIVE_SIZE();
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

            is.close();

            response.setContent(bos.toByteArray());
            response.setHeaders(con.getHeaderFields());
            bos.close();
            if (proxy != null) {
                proxyGenerator.markGood(proxy, url.toString());
            }
            return response;
        } catch (Exception ex) {
            if (proxy != null) {
                proxyGenerator.markBad(proxy, url.toString());
            }
            throw ex;
        }
    }

    public URL getUrl() {
        return url;
    }

    public void setUrl(URL url) {
        this.url = url;
    }

    public RequestConfig getRequestConfig() {
        return requestConfig;
    }

    public void setRequestConfig(RequestConfig requestConfig) {
        this.requestConfig = requestConfig;
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
            LOG.info("Exception",ex);
        }
    }

}
