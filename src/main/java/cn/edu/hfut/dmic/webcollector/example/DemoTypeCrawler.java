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


/**
 * 
 * WebCollector 2.40新特性 page.matchType
 * 在添加CrawlDatum时（添加种子、或在抓取时向next中添加任务），
 * 可以为CrawlDatum设置type信息
 * 
 * type的本质也是meta信息，为CrawlDatum的附加信息
 * 在添加种子或向next中添加任务时，设置type信息可以简化爬虫的开发
 * 
 * 例如在处理列表页时，爬虫解析出内容页的链接，在将内容页链接作为后续任务
 * 将next中添加时，可设置其type信息为content（可自定义），在后续抓取中，
 * 通过page.matchType("content")就可判断正在解析的页面是否为内容页
 * 
 * 设置type的方法主要有3种：
 * 1）添加种子时，addSeed(url,type)
 * 2）向next中添加后续任务时：next.add(url,type)或next.add(links,type)
 * 3）在定义CrawlDatum时：crawlDatum.type(type)
 *
 * @author hu
 */
public class DemoTypeCrawler extends RamCrawler {

    /*
        该教程是DemoMetaCrawler的简化版
        
        该Demo爬虫需要应对豆瓣图书的三种页面：
        1）标签页（taglist，包含图书列表页的入口链接）
        2）列表页（booklist，包含图书详情页的入口链接）
        3）图书详情页（content）
    
        另一种常用的遍历方法可参考TutorialCrawler
     */
    @Override
    public void visit(Page page, CrawlDatums next) {

        if(page.matchType("taglist")){
            //如果是列表页，抽取内容页链接
            //将内容页链接的type设置为content，并添加到后续任务中
             next.add(page.links("table.tagCol td>a"),"booklist");
        }else if(page.matchType("booklist")){
            next.add(page.links("div.info>h2>a"),"content");
        }else if(page.matchType("content")){
            //处理内容页，抽取书名和豆瓣评分
            String title=page.select("h1>span").first().text();
            String score=page.select("strong.ll.rating_num").first().text();
            System.out.println("title:"+title+"\tscore:"+score);
        }
 
    }

    public static void main(String[] args) throws Exception {
        DemoTypeCrawler crawler = new DemoTypeCrawler();
        crawler.addSeed("https://book.douban.com/tag/","taglist");
      

        /*可以设置每个线程visit的间隔，这里是毫秒*/
        //crawler.setVisitInterval(1000);
        /*可以设置http请求重试的间隔，这里是毫秒*/
        //crawler.setRetryInterval(1000);
        crawler.setThreads(30);
        crawler.start(3);
    }

}
