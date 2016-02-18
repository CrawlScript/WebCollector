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

import cn.edu.hfut.dmic.webcollector.crawldb.DBManager;
import cn.edu.hfut.dmic.webcollector.crawler.Crawler;
import cn.edu.hfut.dmic.webcollector.fetcher.Executor;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BerkeleyDBManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;


/**
 * 本教程演示如何利用WebCollector爬取javascript生成的数据
 *
 * @author hu
 */
public class DemoSelenium {

    static {
        //禁用Selenium的日志
        Logger logger = Logger.getLogger("com.gargoylesoftware.htmlunit");
        logger.setLevel(Level.OFF);
    }


    public static void main(String[] args) throws Exception {
        Executor executor=new Executor() {
            @Override
            public void execute(CrawlDatum datum, CrawlDatums next) throws Exception {
                HtmlUnitDriver driver = new HtmlUnitDriver();
                driver.setJavascriptEnabled(true);
                driver.get(datum.getUrl());
                WebElement element=driver.findElementByCssSelector("span#outlink1");
                System.out.println("反链数:"+element.getText());
            }
        };

        //创建一个基于伯克利DB的DBManager
        DBManager manager=new BerkeleyDBManager("crawl");
        //创建一个Crawler需要有DBManager和Executor
        Crawler crawler= new Crawler(manager,executor);
        crawler.addSeed("http://seo.chinaz.com/?host=www.tuicool.com");
        crawler.start(1);
    }

}
