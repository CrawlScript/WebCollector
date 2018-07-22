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
package cn.edu.hfut.dmic.webcollector.example;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.net.OkHttpRequester;
import cn.edu.hfut.dmic.webcollector.plugin.rocks.BreadthCrawler;
import cn.edu.hfut.dmic.webcollector.util.ExceptionUtils;
import com.google.gson.JsonObject;
import okhttp3.MultipartBody;
import okhttp3.Request;
import okhttp3.RequestBody;


/**
 * 本教程演示了如何自定义http请求
 *
 * 有些爬取任务中，可能只有部分URL需要使用POST请求，我们可以利用2.20版本中添 加的MetaData功能，来完成POST请求的定制。
 *
 * 使用MetaData除了可以标记URL是否需要使用POST，还可以存储POST所需的参数信息
 *
 * 教程中还演示了如何定制Cookie、User-Agent等http请求头信息
 *
 * WebCollector中已经包含了org.json的jar包
 *
 * @author hu
 */
public class DemoPostCrawler extends BreadthCrawler {

    /**
     * 
     * 假设我们要爬取三个链接 1)http://www.A.com/index.php 需要POST，并需要POST表单数据username:John
     * 2)http://www.B.com/index.php?age=10 需要POST，数据直接在URL中 ，不需要附带数据 3)http://www.C.com/
     * 需要GET
     */
    public DemoPostCrawler(final String crawlPath, boolean autoParse) {
        super(crawlPath, autoParse);

        addSeed(new CrawlDatum("http://www.A.com/index.php")
                .meta("method", "POST")
                .meta("username", "John"));
        addSeed(new CrawlDatum("http://www.B.com/index.php")
                .meta("method", "POST"));
        addSeed(new CrawlDatum("http://www.C.com/index.php")
                .meta("method", "GET"));

        setRequester(new OkHttpRequester(){
            @Override
            public Request.Builder createRequestBuilder(CrawlDatum crawlDatum) {
                Request.Builder requestBuilder = super.createRequestBuilder(crawlDatum);
                String method = crawlDatum.meta("method");

                // 默认就是GET方式，直接返回原来的即可
                if(method.equals("GET")){
                    return requestBuilder;
                }

                if(method.equals("POST")){
                    RequestBody requestBody;
                    String username = crawlDatum.meta("username");
                    // 如果没有表单数据username，POST的数据直接在URL中
                    if(username == null){
                        requestBody = RequestBody.create(null, new byte[]{});
                    }else{
                        // 根据meta构建POST表单数据
                        requestBody = new MultipartBody.Builder()
                                .setType(MultipartBody.FORM)
                                .addFormDataPart("username", username)
                                .build();
                    }
                    return requestBuilder.post(requestBody);
                }

                //执行这句会抛出异常
                ExceptionUtils.fail("wrong method: " + method);
                return null;
            }
        });


    }



//    @Override
//    public Page getResponse(CrawlDatum crawlDatum) throws Exception {
//        HttpRequest request = new HttpRequest(crawlDatum.url());
//
//        request.setMethod(crawlDatum.meta("method"));
//        String outputData = crawlDatum.meta("outputData");
//        if (outputData != null) {
//            request.setOutputData(outputData.getBytes("utf-8"));
//        }
//        return request.responsePage();
//        /*
//        //通过下面方式可以设置Cookie、User-Agent等http请求头信息
//        request.setCookie("xxxxxxxxxxxxxx");
//        request.setUserAgent("WebCollector");
//        request.addHeader("xxx", "xxxxxxxxx");
//         */
//    }

    @Override
    public void visit(Page page, CrawlDatums next) {
        JsonObject jsonObject = page.jsonObject();
        System.out.println("JSON信息：" + jsonObject);
    }

    /**
     *
     * @param args 参数
     * @throws Exception 异常
     */
    public static void main(String[] args) throws Exception {

        DemoPostCrawler crawler = new DemoPostCrawler("json_crawler", true);
        crawler.start(1);
    }

}
