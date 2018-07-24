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

import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.ram.RamCrawler;
import cn.edu.hfut.dmic.webcollector.util.ExceptionUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

/**
 * Handle 301/302 redirection
 * @author hu
 */
public class DemoAnnotatedRedirectCrawler extends RamCrawler {

    public DemoAnnotatedRedirectCrawler(String keyword, int pageNum) throws Exception {
        for (int pageIndex = 1; pageIndex <= pageNum; pageIndex++) {
            String url = createBingUrl(keyword, pageIndex);
            addSeedAndReturn(url);
        }
    }

    // If the http status code is 301 or 302,
    // you have to obtain the redirected url, which is "Location" header of the http response
    // and add it to subsequent tasks by applying "next.add(redirectedUrl)"
    // Since the page may contains metadata,
    // you have to copy it to the added task by "xxxx.meta(page.copyMeta())"
    @MatchCode(codes = {301, 302})
    public void visitRedirect(Page page, CrawlDatums next){
        try {
            // page.location() may be relative url path
            // we have to construct an absolute url path
            String redirectUrl = new URL(new URL(page.url()), page.location()).toExternalForm();
            next.addAndReturn(redirectUrl).meta(page.copyMeta());
        } catch (MalformedURLException e) {
            //the way to handle exceptions in WebCollector
            ExceptionUtils.fail(e);
        }
    }


    @Override
    public void visit(Page page, CrawlDatums next) {
        System.out.println("this page is not redirected: " + page.url());
    }

    public static void main(String[] args) throws Exception {
        DemoAnnotatedRedirectCrawler crawler = new DemoAnnotatedRedirectCrawler("Web Crawler", 3);
        crawler.start();
    }

    /**
     * construct the Bing Search url by the search keyword and the pageIndex
     * @param keyword
     * @param pageIndex
     * @return the constructed url
     * @throws Exception
     */
    public static String createBingUrl(String keyword, int pageIndex) throws Exception {
        int first = pageIndex * 10 - 9;
        keyword = URLEncoder.encode(keyword, "utf-8");
        return String.format("http://cn.bing.com/search?q=%s&first=%s", keyword, first);
    }

}
