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

import cn.edu.hfut.dmic.webcollector.crawler.DeepCrawler;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;


/**
 * Demo演示爬取搜狗搜索的搜索结果 分页爬取搜狗，并下载搜索结果中的每个网页到本地
 *
 * @author hu
 */
public class DemoSogouCrawler extends DeepCrawler {

    public DemoSogouCrawler(String crawlPath) {
        super(crawlPath);
    }
    
    /*用一个自增id来生成唯一文件名*/
    AtomicInteger id=new AtomicInteger(0);

    @Override
    public Links visitAndGetNextLinks(Page page) {
        String url = page.getUrl();
        /*如果是搜狗的页面*/
        if (Pattern.matches("http://www.sogou.com/web\\?query=.*", url)) {
            Links nextLinks=new Links();
            /*将所有搜索结果条目的超链接返回，爬虫会在下一层爬取中爬取这些链接*/
            nextLinks.addAllFromDocument(page.getDoc(), "h3>a[id^=uigs]");
            return nextLinks;
        }else{
            /*本程序中之可能遇到2种页面，搜狗搜索页面和搜索结果对应的页面,
              所以这个else{}中对应的是搜索结果对应的页面，我们要保存这些页面到本地
            */
            byte[] content=page.getContent();
            try {
                FileUtils.writeFileWithParent("/home/hu/data/sogou/"+id.incrementAndGet()+".html", content);
                System.out.println("save page "+page.getUrl());
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        DemoSogouCrawler crawler = new DemoSogouCrawler("/home/hu/data/wb");
        for (int page = 1; page <= 5; page++) {
            crawler.addSeed("http://www.sogou.com/web?query=" + URLEncoder.encode("编程") + "&page=" + page);
        }
        
        /*遍历中第一层爬取搜狗的搜索结果页面，
          第二层爬取搜索结果对应的页面,
          所以这里要将层数设置为2
        */
        crawler.start(2);
    }

}
