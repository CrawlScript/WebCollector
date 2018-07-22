package cn.edu.hfut.dmic.webcollector.example;


import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.rocks.BreadthCrawler;

public class DemoAnnotatedCrawler extends BreadthCrawler{

    /**
     * 构造一个基于RocksDB的爬虫
     * RocksDB文件夹为crawlPath，crawlPath中维护了历史URL等信息
     * 不同任务不要使用相同的crawlPath
     * 两个使用相同crawlPath的爬虫并行爬取会产生错误
     *
     * @param crawlPath RocksDB使用的文件夹
     * @param autoParse 是否根据设置的正则自动探测新URL
     */
    public DemoAnnotatedCrawler(String crawlPath, boolean autoParse) {
        super(crawlPath, autoParse);
        addSeed("https://blog.csdn.net/", "seed");
        addRegex("https://blog.csdn.net/.*");
    }



    @MatchUrlRegexRule(urlRegexRule = {
            "https://blog.csdn.net/.*"
    })
    @MatchNullType()
    public void visitMain(Page page, CrawlDatums next) {
        System.out.println("this is regex seed");
    }

//    @MatchUrl(urlRegex = "https://blog.csdn.net/.*")
//    public void visitOther(Page page, CrawlDatums next) {
//        System.out.println("this is other");
//    }


//    @MatchUrl(urlRegex = "https://blog.csdn.net/.*")
//    public void visitOther(Page page, CrawlDatums next) {
//        System.out.println("this is other");
//    }


//    @MatchType(types = "seed")
//    public void visitSeed(Page page, CrawlDatums next) {
//        System.out.println("this is type seed");
//    }


    @Override
    public void visit(Page page, CrawlDatums next) {
//        System.out.println("this is default");
    }

    public static void main(String[] args) throws Exception {
        DemoAnnotatedCrawler crawler = new DemoAnnotatedCrawler("crawl", true);
        crawler.start(2);
    }
}
