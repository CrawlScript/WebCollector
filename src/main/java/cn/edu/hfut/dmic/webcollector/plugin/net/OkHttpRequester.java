/*
 * Copyright (C) 2017 hu
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
package cn.edu.hfut.dmic.webcollector.plugin.net;

import cn.edu.hfut.dmic.webcollector.conf.DefaultConfigured;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.Requester;
import okhttp3.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author hu
 */
public class OkHttpRequester extends DefaultConfigured implements Requester{

    protected OkHttpClient client;
    protected HashSet<Integer> successCodeSet;

    public OkHttpRequester addSuccessCode(int successCode){
        successCodeSet.add(successCode);
        return this;
    }
    public OkHttpRequester removeSuccessCode(int successCode){
        successCodeSet.remove(successCode);
        return this;
    }

    protected HashSet<Integer> createSuccessCodeSet(){
        HashSet<Integer> result = new HashSet<Integer>();
        result.add(200);
        result.add(301);
        result.add(302);
        result.add(404);
        return result;
    }


    public OkHttpClient.Builder createOkHttpClientBuilder(){
        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .followRedirects(false)
                .followSslRedirects(false)
                .connectTimeout(getConf().getConnectTimeout(), TimeUnit.MILLISECONDS)
                .readTimeout(getConf().getReadTimeout(), TimeUnit.MILLISECONDS);
        return builder;

    }

    public Request.Builder createRequestBuilder(CrawlDatum crawlDatum){
        Request.Builder builder = new Request.Builder()
                .header("User-Agent",getConf().getDefaultUserAgent())
                .url(crawlDatum.url());

        String defaultCookie = getConf().getDefaultCookie();
        if(defaultCookie != null){
            builder.header("Cookie", defaultCookie);
        }

        return builder;
    }

    public OkHttpRequester() {
        successCodeSet = createSuccessCodeSet();
        client = createOkHttpClientBuilder().build();
    }

    @Override
    public Page getResponse(String url) throws Exception {
        return getResponse(new CrawlDatum(url));
    }

    @Override
    public Page getResponse(CrawlDatum datum) throws Exception {
        Request  request = createRequestBuilder(datum).build();
        Response response = client.newCall(request).execute();

        String contentType = null;
        byte[] content = null;
        String charset = null;

        ResponseBody responseBody = response.body();
        try {
            int code = response.code();
            //设置重定向地址
            datum.code(code);
            datum.location(response.header("Location"));

            if (!successCodeSet.contains(code)) {
//            throw new IOException(String.format("Server returned HTTP response code: %d for URL: %s (CrawlDatum: %s)", code,crawlDatum.url(), crawlDatum.key()));
//            throw new IOException(String.format("Server returned HTTP response code: %d for %s", code, crawlDatum.briefInfo()));
                throw new IOException(String.format("Server returned HTTP response code: %d", code));

            }
            if (responseBody != null) {
                content = responseBody.bytes();
                MediaType mediaType = responseBody.contentType();
                if (mediaType != null) {
                    contentType = mediaType.toString();
                    Charset responseCharset = mediaType.charset();
                    if (responseCharset != null) {
                        charset = responseCharset.name();
                    }
                }
            }

            Page page = new Page(
                    datum,
                    contentType,
                    content
            );
            page.charset(charset);
            page.obj(response);
            return page;
        }finally {
            if(responseBody != null){
                responseBody.close();
            }
        }
    }

    public HashSet<Integer> getSuccessCodeSet() {
        return successCodeSet;
    }

    public void setSuccessCodeSet(HashSet<Integer> successCodeSet) {
        this.successCodeSet = successCodeSet;
    }
}
