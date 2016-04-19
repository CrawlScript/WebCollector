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
import cn.edu.hfut.dmic.webcollector.net.HttpRequest;
import cn.edu.hfut.dmic.webcollector.net.HttpResponse;
import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BreadthCrawler;

import org.json.JSONObject;

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

    public DemoPostCrawler(String crawlPath, boolean autoParse) {
        super(crawlPath, autoParse);
    }

    @Override
    public HttpResponse getResponse(CrawlDatum crawlDatum) throws Exception {
        HttpRequest request = new HttpRequest(crawlDatum.getUrl());
        
        request.setMethod(crawlDatum.meta("method"));
        String outputData=crawlDatum.meta("outputData");
        if(outputData!=null){
            request.setOutputData(outputData.getBytes("utf-8"));
        }
        return request.getResponse();
        /*
        //通过下面方式可以设置Cookie、User-Agent等http请求头信息
        request.setCookie("xxxxxxxxxxxxxx");
        request.setUserAgent("WebCollector");
        request.addHeader("xxx", "xxxxxxxxx");
        */
    }

    @Override
    public void visit(Page page, CrawlDatums next) {
        String jsonStr = page.getHtml();
        JSONObject json = new JSONObject(jsonStr);
        System.out.println("JSON信息：" + json);
    }

    /**
     * 假设我们要爬取三个链接 1)http://www.A.com/index.php 需要POST，并且需要附带数据id=a
     * 2)http://www.B.com/index.php?id=b 需要POST，不需要附带数据 3)http://www.C.com/
     * 需要GET
     *
     * @param args 参数
     * @throws Exception 异常
     */
    public static void main(String[] args) throws Exception {

        DemoPostCrawler crawler = new DemoPostCrawler("json_crawler", true);
        crawler.addSeed(new CrawlDatum("http://www.A.com/index.php")
                .meta("method", "POST")
                .meta("outputData", "id=a"));
        crawler.addSeed(new CrawlDatum("http://www.B.com/index.php")
                .meta("method", "POST"));
        crawler.addSeed(new CrawlDatum("http://www.C.com/index.php")
                .meta("method", "GET"));

        crawler.start(1);
    }

}
