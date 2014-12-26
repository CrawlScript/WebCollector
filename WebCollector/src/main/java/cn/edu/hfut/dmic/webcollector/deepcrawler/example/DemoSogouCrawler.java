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

package cn.edu.hfut.dmic.webcollector.deepcrawler.example;

import cn.edu.hfut.dmic.webcollector.deepcrawler.DeepCrawler;
import cn.edu.hfut.dmic.webcollector.deepcrawler.DeepLinks;
import cn.edu.hfut.dmic.webcollector.deepcrawler.Visitor;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.parser.ParseUtils;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import cn.edu.hfut.dmic.webcollector.util.LogUtils;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;

import java.util.regex.Pattern;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author hu
 */
/*
  用DeepCrawler实现一个爬取搜狗搜索的爬虫
  我们用一个关键词"编程"在搜狗中搜索。希望爬虫可以下载Sogou搜索结果前5页中，搜索结果
  指向的网页。
  DeepCrawler为我们提供了自定义遍历的功能。
  我们通过递归来定义遍历策略。
*/
public class DemoSogouCrawler extends DeepCrawler{
    
    /*定义一个SogouVisitor，这是爬虫遍历的关键*/
    public static class SogouVisitor implements Visitor{

        
        /*
          设计DeepCrawler的时候，要能够分清楚Navigational Page和Target Page
          Navigational Page并不一定包含我们要的数据，但是通过Navigational Page中的链接，我们
          可以直接或间接地找到待爬取页面。
          Target Page就是包含我们想要的数据的页面。
          在本程序中，类似http://www.sogou.com/web?query=xxx的页面就是Navigational Page,
          这些页面中，搜索结果超链接指向的页面，才是我们正真要的页面(Target Page)
          用visitAndGetNextLinks处理Navigational Page的时候，应该返回后续爬取需要的链接列表，
          否则将没有意义。
        */
        @Override
        public DeepLinks visitAndGetNextLinks(Page page) {
            String url=page.getUrl();
            if(Pattern.matches("http://www.sogou.com/web\\?query.*", url)){
                
                /*如果当前访问的页面，是搜狗的页面，抽取页面中的搜索结果链接，返回*/
                page=ParseUtils.parseDocument(page);
                
                DeepLinks nextLinks=new DeepLinks();
                Elements links=page.getDoc().select("h3>a[id^=uigs]");
                for(Element link:links){
                    nextLinks.add(link.attr("abs:href"));
                }
                return nextLinks;
                
            }else{
                /*如果不是搜狗的页面，本程序中，只有可能是搜索结果对应的页面了，保存这些页面*/
                try {
                    FileUtils.writeFileWithParent("/home/hu/data/sogou/"+
                            page.getUrl().hashCode()+".html", page.getContent());
                    LogUtils.getLogger().info("save "+page.getUrl());
                } catch (IOException ex) {
                    LogUtils.getLogger().info("Exception",ex);
                }
                /*搜索结果对应的页面中没有我们需要的链接，所以返回null*/
                return null;
            }
        }
        
    }

    @Override
    public Visitor createVisitor(String url, String contentType) {
        return new SogouVisitor();
    }
    
    public static void main(String[] args) throws Exception{
        DemoSogouCrawler crawler=new DemoSogouCrawler();
        for(int i=1;i<=5;i++){
            crawler.addSeed("http://www.sogou.com/web?query="+URLEncoder.encode("编程")+"&page="+i);
        }
        crawler.setThreads(50);
        
        /*如果不能判断需要遍历几层，start后面的参数，可以设置成一个比较大的值
          本程序中，只需要爬取2层即可完成所有遍历*/
        crawler.start(2);
    }

    
}
