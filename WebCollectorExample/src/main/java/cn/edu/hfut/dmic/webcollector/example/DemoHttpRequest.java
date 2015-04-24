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
package cn.edu.hfut.dmic.webcollector.example;

import cn.edu.hfut.dmic.webcollector.net.HttpRequest;
import cn.edu.hfut.dmic.webcollector.net.HttpResponse;
import cn.edu.hfut.dmic.webcollector.net.RequestConfig;

/**
 *
 * @author hu
 */
public class DemoHttpRequest {

    public static void demo1() throws Exception {
        HttpRequest request = new HttpRequest("http://www.csdn.net");

        HttpResponse response = request.getResponse();
        String html = response.getHtmlByCharsetDetect();

        System.out.println(html);
    }

    public static void demo2() throws Exception {
        RequestConfig requestConfig = new RequestConfig();
        requestConfig.setMethod("GET");
        requestConfig.setUserAgent("WebCollector");
        requestConfig.setCookie("xxxxxxxxxxxxxx");
        requestConfig.addHeader("xxx", "xxxxxxxxx");

        HttpRequest request = new HttpRequest("http://www.csdn.net", requestConfig);

        HttpResponse response = request.getResponse();
        String html = response.getHtmlByCharsetDetect();

        System.out.println(html);
        System.out.println("response code=" + response.getCode());
        System.out.println("Server=" + response.getHeader("Server"));
    }

    public static void main(String[] args) throws Exception {
        demo1();
        demo2();
    }
}
