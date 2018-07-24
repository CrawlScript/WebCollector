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
import cn.edu.hfut.dmic.webcollector.plugin.ram.RamCrawler;
import cn.edu.hfut.dmic.webcollector.util.ExceptionUtils;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

/**
 * 本教程演示了WebCollector 2.20的新特性:
 *  1)MetaData:
 *    MetaData是每个爬取任务的附加信息,灵活应用MetaData可以大大简化爬虫的设计.
 *    例如Post请求往往需要包含参数，而传统爬虫单纯使用URL来保存参数的方法不适合复杂的POST请求.
 *    一些爬取任务希望获取遍历树的深度信息，这也可以通过MetaData轻松实现，可参见教程DemoDepthCrawler
 *    
 *  2)RamCrawler:
 *    RamCrawler不需要依赖文件系统或数据库，适合一次性的爬取任务.
 *    如果希望编写长期任务，请使用BreadthCrawler.
 * 
 * 本教程实现了一个爬取Bing搜索前n页结果的爬虫，爬虫的结果直接输出到标准输出流
 * 如果希望将爬取结果输出到ArrayList等数据结构中，在类中定义一个ArrayList的成员变量，
 * 输出时将结果插入ArrayList即可，这里需要注意的是爬虫是多线程的，而ArrayList不是线程
 * 安全的，因此在执行插入操作时，可使用synchronized(this){ //插入操作}的方式上锁保证安全。
 * 
 * 本教程中对Bing搜索的解析规则可能会随Bing搜索的改版而失效
 * 
 * @author hu
 */
public class DemoAnnotatedBingCrawler extends RamCrawler {

    public DemoAnnotatedBingCrawler(String keyword, int pageNum) throws Exception {
        for (int pageIndex = 1; pageIndex <= pageNum; pageIndex++) {
            String url = createBingUrl(keyword, pageIndex);
            addSeedAndReturn(url)
                    .type("searchEngine")
                    .meta("keyword", keyword)
                    .meta("pageIndex", pageIndex)
                    .meta("depth", 1);
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

    @MatchType(types = "searchEngine")
    public void visitSearchEngine(Page page, CrawlDatums next) {
        String keyword = page.meta("keyword");
        int pageIndex = page.metaAsInt("pageIndex");
        System.out.println("成功抓取关键词" + keyword + "的第" + pageIndex + "页搜索结果");
        Elements results = page.select("li.b_algo>h2>a");

        for (int rank = 0; rank < results.size(); rank++) {
            Element result = results.get(rank);
            /*
            我们希望继续爬取每条搜索结果指向的网页，这里统称为外链。
            我们希望在访问外链时仍然能够知道外链处于搜索引擎的第几页、第几条，
            所以将页号和排序信息放入后续的CrawlDatum中，为了能够区分外链和
            搜索引擎结果页面，type设置为outlink，这里的值完全由
            用户定义，可以设置一个任意的值
            */
            String href = result.attr("abs:href");
            next.addAndReturn(href)
                    .type("outlink")
                    .meta("keyword", keyword)
                    .meta("pageIndex", pageIndex)
                    .meta("rank", rank);
        }
    }

    @MatchType(types = "outlink")
    public void visitOutlink(Page page, CrawlDatums next) {
        int depth = page.metaAsInt("depth");
        int pageIndex = page.metaAsInt("pageIndex");
        int rank = page.metaAsInt("rank");
        String referer=page.meta("referer");

        String line = String.format("第%s页第%s个结果:%s(%s字节)\tdepth=%s\treferer=%s",
                pageIndex, rank + 1, page.doc().title(),page.content().length, depth, referer);
        System.out.println(line);
    }

    @Override
    public void visit(Page page, CrawlDatums next) {

    }

    /*
    在经典爬虫中，每个网页都有一个referer信息，表示当前网页的链接来源。
    例如我们首先访问新浪首页，然后从新浪首页中解析出了新的新闻链接，
    则这些网页的referer值都是新浪首页。WebCollector不直接保存referer值，
    但我们可以通过下面的方式，将referer信息保存在metaData中，达到同样的效果。
    经典爬虫中锚文本的存储也可以通过下面方式实现。

    在一些需求中，希望得到当前页面在遍历树中的深度，利用metaData很容易实现
    这个功能，在将CrawlDatum添加到next中时，将其depth设置为当前访问页面
    的depth+1即可。
     */
    @AfterParse
    public void afterParse(Page page, CrawlDatums next){
        int depth = page.metaAsInt("depth");
        next.meta("depth", depth + 1).meta("referer", page.url());
    }



    public static void main(String[] args) throws Exception {
        DemoAnnotatedBingCrawler crawler = new DemoAnnotatedBingCrawler("网络爬虫", 3);
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
