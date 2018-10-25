package cn.edu.hfut.dmic.webcollector.example;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.net.OkHttpRequester;
import cn.edu.hfut.dmic.webcollector.plugin.rocks.BreadthCrawler;
import okhttp3.Request;

/**
 * 教程：使用WebCollector自定义Http请求
 * 可以自定义User-Agent和Cookie
 *
 * @author hu
 */
public class DemoCookieCrawler extends BreadthCrawler {

    // 自定义的请求插件
    // 可以自定义User-Agent和Cookie
    public static class MyRequester extends OkHttpRequester {

        String userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36";
        String cookie = "name=abcdef";

        // 每次发送请求前都会执行这个方法来构建请求
        @Override
        public Request.Builder createRequestBuilder(CrawlDatum crawlDatum) {
            // 这里使用的是OkHttp中的Request.Builder
            // 可以参考OkHttp的文档来修改请求头
            System.out.println("request with cookie: " + cookie);
            return super.createRequestBuilder(crawlDatum)
                    .header("User-Agent", userAgent)
                    .header("Cookie", cookie);
        }

    }

    public DemoCookieCrawler(String crawlPath) {
        super(crawlPath, true);

        // 设置请求插件
        setRequester(new MyRequester());

        // 爬取github about下面的网页
        addSeed("https://github.com/about");
        addRegex("https://github.com/about/.*");

    }

    public void visit(Page page, CrawlDatums crawlDatums) {
        System.out.println(page.doc().title());
    }

    public static void main(String[] args) throws Exception {
        DemoCookieCrawler crawler = new DemoCookieCrawler("crawl");
        crawler.start(2);
    }
}
