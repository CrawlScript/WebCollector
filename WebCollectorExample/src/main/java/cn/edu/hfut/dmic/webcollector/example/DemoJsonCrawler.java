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

import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BreadthCrawler;
import java.net.HttpURLConnection;
import java.net.ProtocolException;

import org.json.JSONObject;

/**
 * 爬取JSON的例子,
 *
 * 很多JSON爬取必须要设置Cookie、User-Agent 有时候还需要使用POST方法
 *
 * @author hu
 */
public class DemoPostCrawler extends BreadthCrawler {

    public DemoPostCrawler(String crawlPath, boolean autoParse) {
        super(crawlPath, autoParse);
    }

    @Override
    public void visit(Page page, CrawlDatums next) {
        String jsonStr = page.getHtml();
        JSONObject json = new JSONObject(jsonStr);
        System.out.println("JSON信息：" + json);
    }

    public static void main(String[] args) throws Exception {

        DemoPostCrawler crawler = new DemoPostCrawler("json_crawler", true);
        crawler.addSeed("http://abc.com/xxx1.json");
        crawler.addSeed("http://abc.com/xxx2.json");

        crawler.start(1);
    }

}
