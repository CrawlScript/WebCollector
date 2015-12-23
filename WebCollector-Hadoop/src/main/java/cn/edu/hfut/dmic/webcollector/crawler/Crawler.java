/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.crawler;

import cn.edu.hfut.dmic.webcollector.fetcher.Fetcher;
import cn.edu.hfut.dmic.webcollector.fetcher.Visitor;
import cn.edu.hfut.dmic.webcollector.crawldb.DBUpdater;
import cn.edu.hfut.dmic.webcollector.crawldb.Generator;
import cn.edu.hfut.dmic.webcollector.crawldb.Injector;
import cn.edu.hfut.dmic.webcollector.crawldb.Merge;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.Requester;
import cn.edu.hfut.dmic.webcollector.plugin.common.CommonRequester;
import cn.edu.hfut.dmic.webcollector.plugin.common.RegexVisitor;
import cn.edu.hfut.dmic.webcollector.util.CrawlerConfiguration;
import cn.edu.hfut.dmic.webcollector.util.RegexRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


/**
 *
 * @author hu
 */
public class Crawler {
    
    protected Path crawlPath;
    protected Configuration conf;
    
    public Crawler(Path crawlPath, Class<? extends Visitor> visitorClass) {
        this.crawlPath = crawlPath;
        conf = CrawlerConfiguration.create();
        setVisitor(visitorClass);
    }
    
    protected CrawlDatums seeds = new CrawlDatums();
    protected CrawlDatums forcedSeeds = new CrawlDatums();
    
    public void addSeed(String url, boolean force) {
        if (force) {
            forcedSeeds.add(new CrawlDatum(url));
        } else {
            seeds.add(new CrawlDatum(url));
        }
    }
    
    public void addSeed(CrawlDatums datums, boolean force) {
        if (force) {
            forcedSeeds.add(datums);
        } else {
            seeds.add(datums);
        }
    }
    
    public void addSeed(CrawlDatum datum, boolean force) {
        if (force) {
            forcedSeeds.add(datum);
        } else {
            seeds.add(datum);
        }
    }
    
    public void addSeed(String url) {
        addSeed(url, false);
    }
    
    public void addSeed(CrawlDatums datums) {
        addSeed(datums, false);
    }
    
    public void addSeed(CrawlDatum datum) {
        addSeed(datum, false);
    }
    
    public void inject() throws Exception {
        for (CrawlDatum datum : seeds) {
            datum.setStatus(CrawlDatum.STATUS_DB_INJECT);
        }
        for (CrawlDatum datum : forcedSeeds) {
            datum.setStatus(CrawlDatum.STATUS_DB_FORCED_INJECT);
        }
        CrawlDatums injectSeeds = new CrawlDatums().add(seeds).add(forcedSeeds);
        Injector.inject(crawlPath, injectSeeds, conf);
    }
    
    public void setTopN(int topN){
          conf.setInt("generator.topN", topN);
    }
    
    public void start(int depth) throws Exception {
        
        inject();
        
        for (int i = 1; i <= depth; i++) {
            String segmentName = Generator.generate(crawlPath, conf);
            
            if (segmentName == null) {
                System.out.println("no more crawldatums to crawl,stop at depth " + i);
                break;
            }
            Fetcher.fetch(crawlPath, segmentName, conf);
            DBUpdater.updateDB(crawlPath, segmentName, conf);
        }
    }
    
    public void setVisitor(Class<? extends Visitor> visitorClass) {
        conf.set("visitor.class", visitorClass.getName());
    }
    
    public void setRequester(Class<? extends Requester> requesterClass) {
        conf.set("requester.class", requesterClass.getName());
    }
    
    public void setStoreContent(boolean storeContent){
        conf.setBoolean("fetcher.store.content", storeContent);
    }
    
    public static class MyVisitor extends RegexVisitor {
        
        @Override
        public boolean isAutoParse() {
            return true;
        }
        
        @Override
        public void config(RegexRule regexRule) {
            regexRule.addRule("http://.+people.com.cn/.+html");
        }
        
        @Override
        public void visit(Page page, CrawlDatums next) {
            System.out.println("title:" + page.getDoc().title());
        }
        
    }
    
    
    
    public static void main(String[] args) throws Exception {
   
        Crawler crawler = new Crawler(new Path("hdfs://localhost:9000/task1"), MyVisitor.class);
        crawler.addSeed("http://www.people.com.cn/");
        //crawler.addSeed("https://ruby-china.org/Rei");
        crawler.setTopN(5000);
        crawler.start(80);
    }
}
