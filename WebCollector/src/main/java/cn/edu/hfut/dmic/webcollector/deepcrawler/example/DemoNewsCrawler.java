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

import cn.edu.hfut.dmic.htmlbot.contentextractor.ContentExtractor;
import cn.edu.hfut.dmic.webcollector.deepcrawler.DeepCrawler;
import cn.edu.hfut.dmic.webcollector.deepcrawler.Visitor;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.parser.ParseUtils;
import cn.edu.hfut.dmic.webcollector.util.CharsetDetector;

import cn.edu.hfut.dmic.webcollector.util.LogUtils;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author hu
 */
public class DemoNewsCrawler extends DeepCrawler implements Visitor {


    /*用一个全局自增id,来给每个新闻做主键*/
    AtomicInteger id = new AtomicInteger(0);

    @Override
    public ArrayList<String> visitAndGetNextLinks(Page page) {
        String url = page.getUrl();
        if (Pattern.matches("http://news.hfut.edu.cn/list-1-[0-9]+.html", url)) {

            /*如果当前访问的页面，是目录页,抽取新闻链接*/
            page = ParseUtils.parseDocument(page);

            ArrayList<String> nextLinks = new ArrayList<String>();
            Elements links = page.getDoc().select("div>div>ul").first()
                    .select("li>a");
            for (Element link : links) {
                nextLinks.add(link.attr("abs:href"));
            }
            return nextLinks;

        } else {
            /*如果是新闻页面，抽取新闻内容
             这里可以使用page=ParseUtils.parseDocument(page);
             通过getDoc()获取网页的Document(JSOUP的)，也可以用智能抽取。
             这里使用正文智能抽取ContentExtractor
             */
            try {
                String charset = CharsetDetector.guessEncoding(page.getContent());
                String html = new String(page.getContent(), charset);
                String text = ContentExtractor.getContentByHtml(html);
                LogUtils.getLogger().info("网页URL:" + page.getUrl() + "   网页ID:" + id.incrementAndGet());
                LogUtils.getLogger().info("网页正文:\n" + text);
            } catch (Exception ex) {
                LogUtils.getLogger().info("Exception", ex);
            }
            /*新闻页面中没有我们需要的链接，所以返回null*/
            return null;
        }
    }

    @Override
    public Visitor createVisitor(String url, String contentType) {
        return this;
    }

    public static void main(String[] args) throws Exception {
        DemoNewsCrawler crawler = new DemoNewsCrawler();
        for (int i = 1; i <= 5; i++) {
            crawler.addSeed("http://news.hfut.edu.cn/list-1-" + i + ".html");
        }
        crawler.setThreads(50);

        /*如果不能判断需要遍历几层，start后面的参数，可以设置成一个比较大的值
         本程序中，只需要爬取2层即可完成所有遍历*/
        crawler.start(2);
    }
}
