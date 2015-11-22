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

import cn.edu.hfut.dmic.webcollector.net.HttpResponse;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.ram.RamCrawler;
import java.io.IOException;
import java.net.URL;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author hu
 */
public class DemoSeleniumHttpRequest {
    
}

/*
import org.openqa.selenium.htmlunit.HtmlUnitDriver;

public class DemoSeleniumHttpRequest {

    static {

        Logger logger = Logger.getLogger("com.gargoylesoftware.htmlunit");
        logger.setLevel(Level.OFF);
    }

    public HttpResponse getResponse(CrawlDatum datum) throws Exception {
        HtmlUnitDriver driver = new HtmlUnitDriver();

        driver.setJavascriptEnabled(true);

        try {
            driver.get(datum.getUrl());
            HttpResponse response = new HttpResponse(new URL(datum.getUrl()));
            response.setCode(200);
            String html = driver.getPageSource();
            response.setHtml(html);
            response.addHeader("Content-Type", "text/html");
            return response;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }

    }

    public static void main(String[] args) throws IOException, Exception {

        RamCrawler crawler = new RamCrawler(false) {
            @Override
            public HttpResponse getResponse(CrawlDatum crawlDatum) throws Exception {
                return new DemoSeleniumHttpRequest().getResponse(crawlDatum);
            }

            @Override
            public void visit(Page page, CrawlDatums next) {
                System.out.println(page.select("span#outlink1").first().text());
            }
        };
        crawler.addSeed("http://seo.chinaz.com/?host=www.tuicool.com");
        crawler.start(1);

    }

}
*/