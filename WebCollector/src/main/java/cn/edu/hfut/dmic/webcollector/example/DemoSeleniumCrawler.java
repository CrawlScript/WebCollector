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

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.HttpResponse;
import cn.edu.hfut.dmic.webcollector.plugin.ram.RamCrawler;

/**
 *
 * @author hu
 */
//public class DemoSeleniumCrawler {
//    
//}
/**
 * 本教程演示如何利用WebCollector爬取javascript生成的数据
 * 由于使用的selenium组件jar包过大，已在pom.xml中注释selenium的dependency，
 * 但本教程依赖selenium，为了能够编译通过，将本教程代码注释
 * 如果希望编译此教程，请先取消pom.xml中selenium的dependency的注释，重新构建工程后， 取消此教程的注释即可
 *
 * @author hu
 */
public class DemoSeleniumCrawler{
    
}
/*
public class DemoSeleniumCrawler extends RamCrawler {

    @Override
    public HttpResponse getResponse(CrawlDatum crawlDatum) throws Exception {
        return new DemoSeleniumHttpRequest().getResponse(crawlDatum);
    }

    @Override
    public void visit(Page page, CrawlDatums next) {
        //span#outlink1表示网页中id为outlink1的span元素
        //"span#outlink1"中的信息是由javascript加载的
        System.out.println("反链数:"+page.select("span#outlink1").first().text());
    }

    public static void main(String[] args) throws Exception {
        DemoSeleniumCrawler crawler = new DemoSeleniumCrawler();
        crawler.addSeed("http://seo.chinaz.com/?host=www.tuicool.com");
        crawler.start(1);
    }

}
*/