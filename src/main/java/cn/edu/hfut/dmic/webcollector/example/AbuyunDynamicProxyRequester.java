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

import cn.edu.hfut.dmic.webcollector.plugin.net.OkHttpRequester;
import okhttp3.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;

/**
 * WebCollector使用阿布云代理的Http请求插件
 * <p>
 * 插件使用方法：
 * crawler.setRequester(new AbuyunDynamicProxyRequester("阿布云动态代理用户名", "阿布云动态代理密码"));
 * 一行代码解决，无需自定义代理池及其它代理切换规则
 * <p>
 * 阿布云代理官网：
 * https://www.abuyun.com/
 *
 * @author hu
 */
public class AbuyunDynamicProxyRequester extends OkHttpRequester {

    String credential;

    public AbuyunDynamicProxyRequester(String proxyUser, String proxyPass) {
        credential = Credentials.basic(proxyUser, proxyPass);
        removeSuccessCode(301);
        removeSuccessCode(302);
    }

    @Override
    public OkHttpClient.Builder createOkHttpClientBuilder() {
        String proxyHost = "proxy.abuyun.com";
        int proxyPort = 9020;
        Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
        return super.createOkHttpClientBuilder()
                .proxy(proxy)
                .proxyAuthenticator(new Authenticator() {
                    @Override
                    public Request authenticate(Route route, Response response) throws IOException {
                        return response.request().newBuilder()
                                .header("Proxy-Authorization", credential)
                                .build();
                    }
                });
    }

    public static void main(String[] args) throws Exception {
        final String username = "阿布云动态代理用户名";
        final String password = "阿布云动态代理密码";
        AbuyunDynamicProxyRequester requester = new AbuyunDynamicProxyRequester(username, password);
        System.out.println(requester.getResponse("https://github.com/").html());
    }
}
