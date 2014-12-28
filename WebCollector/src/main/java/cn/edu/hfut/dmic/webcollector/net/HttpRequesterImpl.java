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

/**
 *
 * @author hu
 */
public class HttpRequesterImpl implements HttpRequester {

    

    protected Proxys proxys = null;

    protected String userAgent = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0";
    protected String cookie = null;

    public void configConnection(HttpURLConnection con) {

    }

    @Override
    public HttpResponse getResponse(String url) throws Exception {
        HttpResponse response = new HttpResponse(url);
        HttpURLConnection con;
        URL _URL = new URL(url);
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

        if (userAgent != null) {
            con.setRequestProperty("User-Agent", userAgent);
        }
        if (cookie != null) {
            con.setRequestProperty("Cookie", cookie);
        }

        con.setDoInput(true);
        con.setDoOutput(true);

        configConnection(con);

        InputStream is;
        response.setCode(con.getResponseCode());
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
        HttpResponse response = requester.getResponse("http://www.baidu.com");
        System.out.println(response.getCode());
    }

}
