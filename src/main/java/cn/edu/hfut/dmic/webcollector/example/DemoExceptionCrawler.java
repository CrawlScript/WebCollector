package cn.edu.hfut.dmic.webcollector.example;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.rocks.BreadthCrawler;
import cn.edu.hfut.dmic.webcollector.util.ExceptionUtils;

/**
 * Crawling news from github news
 *
 * @author hu
 */
public class DemoExceptionCrawler extends BreadthCrawler {
    /**
     * @param crawlPath crawlPath is the path of the directory which maintains
     *                  information of this crawler
     * @param autoParse if autoParse is true,BreadthCrawler will auto extract
     *                  links which match regex rules from pag
     */
    public DemoExceptionCrawler(String crawlPath, boolean autoParse) {
        super(crawlPath, autoParse);
        /*start pages*/
        this.addSeed("https://blog.github.com/");
    }

    public void myMethod() throws Exception{
        throw new Exception("this is an exception");
    }

    @Override
    public void visit(Page page, CrawlDatums next) {
        try {
            this.myMethod();
        } catch (Exception e) {
            // 当捕捉到异常时，且认为这个网页需要重新爬取时
            // 应该使用ExceptionUtils.fail(e)
            // 无视或者throw异常在编译时会报错，因为visit方法没有throws异常
            // 该方法会抛出RuntimeException，不会强制要求visit方法加上throws
            ExceptionUtils.fail(e);
        }
    }

    public static void main(String[] args) throws Exception {
        DemoExceptionCrawler crawler = new DemoExceptionCrawler("crawl", true);
        /*start crawl with depth of 4*/
        crawler.start(4);
    }

}