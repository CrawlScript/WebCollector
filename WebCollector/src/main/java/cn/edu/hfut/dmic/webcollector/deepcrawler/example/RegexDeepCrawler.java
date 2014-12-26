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

import cn.edu.hfut.dmic.webcollector.deepcrawler.*;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.parser.ParseUtils;
import cn.edu.hfut.dmic.webcollector.util.LogUtils;
import cn.edu.hfut.dmic.webcollector.util.RegexRule;
import java.util.ArrayList;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author hu
 */
/*用DeepCrawler来实现广度遍历*/
public class RegexDeepCrawler extends DeepCrawler implements Visitor {

    protected RegexRule regexRule = new RegexRule();

    @Override
    public DeepLinks visitAndGetNextLinks(Page page) {


        /*
         在这之前，page的getHtml()和getDoc()都为null,但是可以通过
         page.getUrl()来获取页面url,以及通过page.getContent()
         来获取网页源码的byte[]，可以通过网页的url和content,来生成
         页面的html源码字符串(需要检测网页的字符集),同时也可以生成网
         页的DOM树，ParseUtils.parseDocument为用户封装了一套自动
         检测网页字符集，以及利用JSOUP生成DOM树的方法，用户可以仿照
         源码自己编写*/
        page = ParseUtils.parseDocument(page);
        try {
            visit(page);
        } catch (Exception ex) {
            LogUtils.getLogger().info("Exception",ex);
        }

        /*抽取所有的链接，放入后续任务*/
        Elements links = page.getDoc().select("a[href]");
        DeepLinks nextUrls = new DeepLinks();
        for (Element link : links) {
            /*abs:href表示抽取超链接，并转换为绝对地址，否则无法
             处理类似<a href='/help.html'>help</a>的链接*/
            String href = link.attr("abs:href");
            /*如果链接URL满足正则条件，加入后续任务*/
            if (regexRule.satisfy(href)) {
                nextUrls.add(href);
            }
        }

        /*
         nextUrls为后续任务中需要爬取的URL，
         不用担心多个页面可能生成重复的URL，系统会
         进行URL去重
         */
        return nextUrls;

    }

    @Override
    public Visitor createVisitor(String url, String contentType) {
        if (contentType == null) {
            return null;
        }
        if (contentType.contains("text/html")) {
            return this;
        }
        return null;
    }

    /**
     * 添加一个正则过滤规则
     *
     * @param regex 正则过滤规则
     */
    public void addRegex(String regex) {
        regexRule.addRule(regex);
    }

    public RegexRule getRegexRule() {
        return regexRule;
    }

    public void setRegexRule(RegexRule regexRule) {
        this.regexRule = regexRule;
    }

    public void visit(Page page) {
        
    }

    public static void main(String[] args) throws Exception {

        RegexDeepCrawler crawler = new RegexDeepCrawler(){

            @Override
            public void visit(Page page) {
                LogUtils.getLogger().info("title:" + page.getDoc().title());
            }
            
        };
        crawler.addSeed("http://www.hfut.edu.cn/ch/");
        crawler.addRegex("http://.*hfut.edu.cn/.*");
        crawler.start(5);

    }

}
