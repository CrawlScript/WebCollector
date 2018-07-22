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
import cn.edu.hfut.dmic.webcollector.plugin.rocks.BreadthCrawler;

/**
 * 本教程和深度遍历没有任何关系
 * 一些爬取需求希望加入深度信息，即遍历树中网页的层
 * 利用2.20版本中的新特性MetaData可以轻松实现这个功能
 *
 * @author hu
 */
public class DemoAnnotatedDepthCrawler extends BreadthCrawler {

    public DemoAnnotatedDepthCrawler(String crawlPath, boolean autoParse) {
        super(crawlPath, autoParse);

        for (int i = 1; i <= 5; i++) {
            addSeed(new CrawlDatum("http://news.hfut.edu.cn/list-1-" + i + ".html")
                    .meta("depth", 1));
        }

        /*正则规则用于控制爬虫自动解析出的链接，用户手动添加的链接，例如添加的种子、或
          在visit方法中添加到next中的链接并不会参与正则过滤*/
        /*自动爬取类似"http://news.hfut.edu.cn/show-xxxxxxhtml"的链接*/
        addRegex("http://news.hfut.edu.cn/show-.*html");
        /*不要爬取jpg|png|gif*/
        addRegex("-.*\\.(jpg|png|gif).*");
        /*不要爬取包含"#"的链接*/
        addRegex("-.*#.*");

    }

    @Override
    public void visit(Page page, CrawlDatums next) {
        System.out.println("visiting:" + page.url() + "\tdepth=" + page.meta("depth"));
    }

    @AfterParse
    public void afterParse(Page page, CrawlDatums next) {
        //当前页面的depth为x，则从当前页面解析的后续任务的depth为x+1
        int depth = 1;
        //如果在添加种子时忘记添加depth信息，可以通过这种方式保证程序不出错
        try {
            depth = page.metaAsInt("depth");
        } catch (Exception ex) {

        }
        depth++;
        next.meta("depth", depth);
    }


    public static void main(String[] args) throws Exception {
        DemoAnnotatedDepthCrawler crawler = new DemoAnnotatedDepthCrawler("crawl", true);
        crawler.getConf().setTopN(5);
        crawler.start(3);
    }

}
