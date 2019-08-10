package cn.edu.hfut.dmic.webcollector.example;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.net.OkHttpRequester;
import cn.edu.hfut.dmic.webcollector.plugin.rocks.BreadthCrawler;
import okhttp3.Request;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;

import java.util.List;

/**
 * 使用WebCollector自定义HTTP请求，并抓取JavaScript生成的数据
 * 由DemoCookieCrawler和DemoSeleniumCrawler的内容结合起来实现
 *
 * @author smallyu
 */
public class DemoSeleniumWithCookieCrawler extends BreadthCrawler {

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

    public DemoSeleniumWithCookieCrawler(String crawlPath) {
        super(crawlPath, true);

        // 设置请求插件
        setRequester(new MyRequester());

        addSeed("https://www.sogou.com/web?query=%E6%B7%98%E5%AE%9D");
    }

    // BreadthCrawler继承自AutoParseCrawler，而AutoParseCrawler本身就是一个Executor
    @Override
    public void execute(CrawlDatum datum, CrawlDatums next) throws Exception {
        super.execute(datum, next);

        // 来自 DemoSeleniumCrawler 的示例代码
        HtmlUnitDriver driver = new HtmlUnitDriver();
        driver.setJavascriptEnabled(true);

        driver.get(datum.url());

        List<WebElement> elementList = driver.findElementsByCssSelector("h3.vrTitle a");
        for(WebElement element:elementList){
            System.out.println("title:"+element.getText());
        }
    }

    // 指定Executor就不需要visit来处理内容了
    public void visit(Page page, CrawlDatums crawlDatums) {}

    public static void main(String[] args) throws Exception {
        DemoSeleniumWithCookieCrawler crawler = new DemoSeleniumWithCookieCrawler("crawl");
        crawler.start(2);
    }
}
