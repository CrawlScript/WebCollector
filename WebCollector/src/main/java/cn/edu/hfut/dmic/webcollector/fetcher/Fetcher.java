/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.fetcher;

import cn.edu.hfut.dmic.webcollector.generator.CollectionGenerator;
import cn.edu.hfut.dmic.webcollector.generator.DbUpdater;
import cn.edu.hfut.dmic.webcollector.generator.Generator;
import cn.edu.hfut.dmic.webcollector.handler.Handler;
import cn.edu.hfut.dmic.webcollector.handler.Message;
import cn.edu.hfut.dmic.webcollector.model.Content;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.parser.ParseResult;
import cn.edu.hfut.dmic.webcollector.parser.Parser;
import cn.edu.hfut.dmic.webcollector.parser.ParserFactory;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import cn.edu.hfut.dmic.webcollector.util.HandlerUtils;
import cn.edu.hfut.dmic.webcollector.util.HttpUtils;
import cn.edu.hfut.dmic.webcollector.util.Log;
import cn.edu.hfut.dmic.webcollector.util.Task;
import cn.edu.hfut.dmic.webcollector.util.WorkQueue;
import java.io.IOException;
import java.net.Proxy;
import java.util.List;


/**
 *
 * @author hu
 */
public class Fetcher extends Task {
    
    public int retry=3;
    
    public Proxy proxy=null;
    

    public static final int FETCH_SUCCESS = 1;
    public static final int FETCH_FAILED = 2;

    int threads = 10;
    String crawl_path;
    String segment_path; 
    boolean isContentStored=true;
    boolean parsing=true;
    
    
    public Fetcher(String crawl_path) {
        this.crawl_path = crawl_path;
    }

    public boolean needUpdateDb=true;
    public Fetcher() {
        needUpdateDb=false;
    }
    
    public DbUpdater dbUpdater = null;
    public SegmentWriter segmengwriter=null;


    private void start() throws IOException {
        if (needUpdateDb) {
            this.dbUpdater = new DbUpdater(crawl_path);
            dbUpdater.setTaskname(this.taskname);
            dbUpdater.initUpdater();
            dbUpdater.lock();
            
            
            
            segment_path=crawl_path+"/segments/"+SegmentWriter.createSegmengName();
            segmengwriter=new SegmentWriter(segment_path);
        }
        workqueue = new WorkQueue(threads);

    }

    public void fetchAll(Generator generator) throws IOException {
        start();
        CrawlDatum crawlDatum = null;
        while ((crawlDatum = generator.next()) != null) {
            addFetcherThread(crawlDatum.url);
        }
        end();

    }

    public void stop() throws IOException {
        workqueue.killALl();
        if (needUpdateDb) {
            dbUpdater.closeUpdater();
            dbUpdater.merge(segment_path);
            dbUpdater.unlock();
        }
    }

    WorkQueue workqueue;

    private void end() throws IOException {
        try {
            while (workqueue.isAlive()) {
                Thread.sleep(5000);
            }
            workqueue.killALl();

        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        if (needUpdateDb) {
            dbUpdater.closeUpdater();
            dbUpdater.merge(segment_path);
            dbUpdater.unlock();
            segmengwriter.close();
        }
    }

    public void addFetcherThread(String url) {
        FetcherThread fetcherthread = new FetcherThread(url);
        workqueue.execute(fetcherthread);
    }

    ConnectionConfig conconfig = null;

    public Handler handler = null;

    class FetcherThread extends Thread {

        String url;

        public FetcherThread(String url) {
            this.url = url;
        }

        @Override
        public void run() {
            Page page = new Page();
            page.url = url;
            Page response=null;
           
            response = HttpUtils.fetchHttpResponse(url,proxy, conconfig, retry);         

            if (response == null) {
                Log.Errors("failed ", Fetcher.this.taskname, page.url);
                HandlerUtils.sendMessage(handler, new Message(Fetcher.FETCH_FAILED, page),true);              
                return;
            }
            
            page=response;

            CrawlDatum crawldatum = new CrawlDatum();
            crawldatum.url=url;
            crawldatum.status = Page.FETCHED;
            page.fetchtime = System.currentTimeMillis();
            crawldatum.fetchtime = page.fetchtime;
            Log.Infos("fetch", Fetcher.this.taskname, page.url);

            if (needUpdateDb) {              
                try {
                    segmengwriter.wrtieFetch(crawldatum);
                    String contentType;                   
                    List<String> contentTypeList=page.headers.get("Content-Type");
                    if(contentTypeList==null){
                        contentType=null;
                    }else{
                        contentType=contentTypeList.get(0);
                    }
                    
                    if(isContentStored){
                        Content content=new Content();
                        content.url=page.url;
                        if(page.content!=null){
                            content.content=page.content;
                        }else{
                            content.content=new byte[0];
                        }
                        content.contentType=contentType;                       
                        segmengwriter.wrtieContent(content);
                    }
                    if(parsing){
                        Parser parser=ParserFactory.getParser(contentType, page.url);       
                        if(parser!=null){
                            ParseResult parseresult = parser.getParse(page);
                            page.parseResult=parseresult;
                            segmengwriter.wrtieParse(parseresult);                          
                        }
                    }
                      

                    
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            
            HandlerUtils.sendMessage(handler, new Message(Fetcher.FETCH_SUCCESS, page),true);
           
        }
    }

    public ConnectionConfig getConconfig() {
        return conconfig;
    }

    public void setConconfig(ConnectionConfig conconfig) {
        this.conconfig = conconfig;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }

    public boolean getNeedUpdateDb() {
        return needUpdateDb;
    }

    public void setNeedUpdateDb(boolean needUpdateDb) {
        this.needUpdateDb = needUpdateDb;
    }

    

    public int getRetry() {
        return retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public boolean isIsContentStored() {
        return isContentStored;
    }

    public void setIsContentStored(boolean isContentStored) {
        this.isContentStored = isContentStored;
    }

    public boolean isParsing() {
        return parsing;
    }

    public void setParsing(boolean parsing) {
        this.parsing = parsing;
    }

    public Proxy getProxy() {
        return proxy;
    }

    public void setProxy(Proxy proxy) {
        this.proxy = proxy;
    }
    
    
    

    
    public static void main(String[] args) throws IOException{
        CollectionGenerator generator=new CollectionGenerator();
        generator.addUrl("http://www.hfut.edu.cn/ch/");
        generator.addUrl("http://news.hfut.edu.cn/");
        Fetcher fetcher=new Fetcher();
        fetcher.fetchAll(generator);
        
        
    }
    
    

}
