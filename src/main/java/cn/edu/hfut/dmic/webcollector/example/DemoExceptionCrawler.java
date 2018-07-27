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

        // the ExecuteCount will increate by 1 when the task(CrawlDatum) is executed
        // either the task(CrawlDatum) succeed or fail
        // MaxExecuteCount limit the max execution number of the task(CrawlDatum)
        getConf().setMaxExecuteCount(3);
    }

    public void myMethod() throws Exception{
        throw new Exception("this is an exception");
    }

    @Override
    public void visit(Page page, CrawlDatums next) {
        try {
            this.myMethod();
        } catch (Exception e) {
            // if you want to throw exceptions manually in the visit method
            // you should use ExceptionUtils.fail and the crawler will consider the task(CrawlDatum) as failed
            // failed tasks(CrawlDatums) will be executed again if the MaxExecuteCount is not reached
            ExceptionUtils.fail(e);
        }
    }

    public static void main(String[] args) throws Exception {
        DemoExceptionCrawler crawler = new DemoExceptionCrawler("crawl", true);
        /*start crawl with depth of 4*/
        crawler.start(4);

    }

}