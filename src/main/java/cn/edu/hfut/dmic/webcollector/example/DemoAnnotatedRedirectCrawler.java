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

import java.net.URLEncoder;

/**
 * Handle 301/302 redirection
 * @author hu
 */
public class DemoAnnotatedRedirectCrawler extends RamCrawler {

    public DemoAnnotatedRedirectCrawler(String keyword, int maxPageNum) throws Exception {
        for (int pageNum = 1; pageNum <= maxPageNum; pageNum++) {
            String url = createBingUrl(keyword, pageNum);
            addSeedAndReturn(url);
        }
    }

    // 如果遇到301或者302，手动跳转（将任务加到next中）
    // 并且复制任务的meta
    @MatchCode(codes = {301, 302})
    public void visitRedirect(Page page, CrawlDatums next){
        next.addAndReturn(page.location()).meta(page.meta().deepCopy());
    }


    @Override
    public void visit(Page page, CrawlDatums next) {
        System.out.println("this page is not redirected: " + page.url());
    }

    public static void main(String[] args) throws Exception {
        DemoAnnotatedRedirectCrawler crawler = new DemoAnnotatedRedirectCrawler("网络爬虫", 3);
        crawler.start();
    }

    /**
     * 根据关键词和页号拼接Bing搜索对应的URL
     * @param keyword 关键词
     * @param pageNum 页号
     * @return 对应的URL
     * @throws Exception 异常 
     */
    public static String createBingUrl(String keyword, int pageNum) throws Exception {
        int first = pageNum * 10 - 9;
        keyword = URLEncoder.encode(keyword, "utf-8");
        return String.format("http://cn.bing.com/search?q=%s&first=%s", keyword, first);
    }

}
