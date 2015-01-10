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

import cn.edu.hfut.dmic.webcollector.util.Config;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class HttpRequesterImpl implements HttpRequester {
    public static final Logger LOG = LoggerFactory.getLogger(HttpRequesterImpl.class);
    protected Proxys proxys = null;
    protected String userAgent = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0";
    protected String cookie = null;

    public void configConnection(HttpURLConnection con) {

    }

    @Override
    public HttpResponse getResponse(String url) throws Exception {
        HttpResponse response = new HttpResponse(url);
        HttpURLConnection con = null;
        URL _URL = new URL(url);
        int code=-1;
        int maxRedirect=Math.max(0, Config.MAX_REDIRECT);
        for (int redirect = 0; redirect <= maxRedirect; redirect++) {
            if (proxys == null) {
                con = (HttpURLConnection) _URL.openConnection();
            } else {
                Proxy proxy = proxys.nextProxy();
                if (proxy == null) {
                    con = (HttpURLConnection) _URL.openConnection();
                } else {
                    con = (HttpURLConnection) _URL.openConnection(proxy);
                }
            }
            //con.setInstanceFollowRedirects(false);
            if (userAgent != null) {
                con.setRequestProperty("User-Agent", userAgent);
            }
            if (cookie != null) {
                con.setRequestProperty("Cookie", cookie);
            }
            con.setInstanceFollowRedirects(false);
            con.setDoInput(true);
            con.setDoOutput(true);
            con.setConnectTimeout(3000);
            con.setReadTimeout(10000);

            configConnection(con);
            code = con.getResponseCode();
            /*只记录第一次返回的code*/
            if(redirect==0){
                response.setCode(code);
            }

            switch (code) {
                case HttpURLConnection.HTTP_MOVED_PERM:
                case HttpURLConnection.HTTP_MOVED_TEMP:  
                    response.setRedirect(true);
                    if(redirect==Config.MAX_REDIRECT){
                        throw new Exception("redirect to much time");
                    }
                    String location = con.getHeaderField("Location");
                    if(location==null){
                        throw new Exception("redirect with no location");
                    }
                    String originUrl=_URL.toString();
                    _URL = new URL(_URL, location);
                    response.setRealUrl(_URL.toString());
                    LOG.info("redirect from "+originUrl+" to "+_URL.toString());
                    continue;
                default:
                    break;
            }

        }    
        
        InputStream is;
        
        is = con.getInputStream();

        byte[] buf = new byte[2048];
        int read;
        int sum = 0;
        int maxsize = Config.MAX_RECEIVE_SIZE;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while ((read = is.read(buf)) != -1) {
            if (maxsize > 0) {
                sum = sum + read;
                if (sum > maxsize) {
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
        return response;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getCookie() {
        return cookie;
    }

    public void setCookie(String cookie) {
        this.cookie = cookie;
    }

    public Proxys getProxys() {
        return proxys;
    }

    public void setProxys(Proxys proxys) {
        this.proxys = proxys;
    }

    public static void main(String[] args) throws Exception {
        HttpRequesterImpl requester = new HttpRequesterImpl();
        HttpResponse response = requester.getResponse("http://localhost/test/haha.php");
        System.out.println(response.getCode());
        System.out.println(new String(response.getContent(), "utf-8"));
    }

}
