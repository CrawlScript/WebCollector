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
import cn.edu.hfut.dmic.webcollector.net.Request;
import cn.edu.hfut.dmic.webcollector.net.RequestFactory;
import cn.edu.hfut.dmic.webcollector.net.Response;
import cn.edu.hfut.dmic.webcollector.parser.ParseResult;
import cn.edu.hfut.dmic.webcollector.parser.Parser;
import cn.edu.hfut.dmic.webcollector.parser.ParserFactory;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import cn.edu.hfut.dmic.webcollector.util.HandlerUtils;

import cn.edu.hfut.dmic.webcollector.util.Log;
import cn.edu.hfut.dmic.webcollector.util.Task;

import java.io.IOException;
import java.net.Proxy;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;



/**
 *
 * @author hu
 */
public class Fetcher extends Task {

    private int retry = 3;

    private Proxy proxy = null;

    private AtomicInteger activeThreads = new AtomicInteger(0);

    private QueueFeeder feeder;
    private FetchQueue fetchQueue;
    
    
    private DbUpdater dbUpdater = null;
    private SegmentWriter segmengwriter = null;
    
    private ConnectionConfig conconfig = null;

    private Handler handler = null;
    
    private boolean needUpdateDb = true;

    public static class FetchItem {

        public CrawlDatum datum;

        public FetchItem(CrawlDatum datum) {
            this.datum = datum;
        }

    }

    public static class FetchQueue {

        public AtomicInteger totalSize = new AtomicInteger(0);
        public List<FetchItem> queue = Collections.synchronizedList(new LinkedList<FetchItem>());

        public int getSize() {
            return queue.size();
        }

        public void addFetchItem(FetchItem item) {
            if (item == null) {
                return;
            }
            queue.add(item);
            totalSize.incrementAndGet();
        }

        public synchronized FetchItem getFetchItem() {
            if (queue.size() == 0) {
                return null;
            }
            return queue.remove(0);
        }

    }

    public static class QueueFeeder extends Thread {

        public FetchQueue queue;
        public Generator generator;
        public int size;

        public QueueFeeder(FetchQueue queue, Generator generator, int size) {
            this.queue = queue;
            this.generator = generator;
            this.size = size;
        }

        @Override
        public void run() {

            boolean hasMore = true;
            while (hasMore) {

                int feed = size - queue.getSize();
                if (feed <= 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                    }
                    continue;
                }
                while (feed > 0 && hasMore) {

                    CrawlDatum datum = generator.next();
                    hasMore = (datum != null);

                    if (hasMore) {
                        queue.addFetchItem(new FetchItem(datum));
                        feed--;
                    }

                }

            }

        }

    }

    private class FetcherThread extends Thread {

        @Override
        public void run() {
            activeThreads.incrementAndGet();
            try {

                while (true) {
                    FetchItem item = fetchQueue.getFetchItem();
                    if (item == null) {
                        if (feeder.isAlive() || fetchQueue.getSize() > 0) {
                            try {
                                Thread.sleep(500);
                            } catch (Exception ex) {
                            }
                            continue;
                        } else {
                            return;
                        }
                    }
                    
                    CrawlDatum crawldatum = new CrawlDatum();
                    String url=item.datum.getUrl();
                    crawldatum.setUrl(url);

                    Request request=RequestFactory.createRequest(url,proxy,conconfig);
                    Response response=null;
                    for(int i=0;i<=retry;i++){
                        try{
                            response=request.getResponse(crawldatum);
                            break;
                        }catch(Exception ex){
                            
                        }
                    }
                    
                    
                    crawldatum.setStatus(CrawlDatum.STATUS_DB_FETCHED);
                    crawldatum.setFetchTime(System.currentTimeMillis());
                    
                    Page page=new Page();
                    page.setUrl(url);
                    page.setFetchtime(crawldatum.getFetchTime());
                    
                    
                    
                    if(response==null){                       
                        Log.Errors("failed ", Fetcher.this.getTaskName(), url);
                        HandlerUtils.sendMessage(handler, new Message(Fetcher.FETCH_FAILED, page), true);
                        continue;
                    }
                    
                    page.setResponse(response);

                      
                    Log.Infos("fetch", Fetcher.this.getTaskName(), url);
                    

                    if (needUpdateDb) {
                        try {
                            segmengwriter.wrtieFetch(crawldatum);
                            String contentType;
                            List<String> contentTypeList = response.getHeader("Content-Type");
                            if (contentTypeList == null) {
                                contentType = null;
                            } else {
                                contentType = contentTypeList.get(0);
                            }

                            if (isContentStored) {
                                Content content = new Content();
                                content.setUrl(url);
                                if (response.getContent() != null) {
                                    content.setContent(response.getContent());
                                } else {
                                    content.setContent(new byte[0]);
                                }
                                content.setContentType(contentType);
                                segmengwriter.wrtieContent(content);
                            }
                            if (parsing) {
                                Parser parser = ParserFactory.getParser(contentType, page.getUrl());
                                if (parser != null) {
                                    ParseResult parseresult = parser.getParse(page);
                                    page.setParseResult(parseresult);
                                    segmengwriter.wrtieParse(parseresult);
                                }
                            }

                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }

                    }

                    HandlerUtils.sendMessage(handler, new Message(Fetcher.FETCH_SUCCESS, page), true);

                }

            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                activeThreads.decrementAndGet();
            }

        }

    }

    public static final int FETCH_SUCCESS = 1;
    public static final int FETCH_FAILED = 2;

    int threads = 10;
    String crawl_path;
    String segment_path;
    boolean isContentStored = true;
    boolean parsing = true;

    public Fetcher(String crawl_path) {
        this.crawl_path = crawl_path;
    }

    

    public Fetcher() {
        needUpdateDb = false;
    }

    

    private void start() throws IOException {
        if (needUpdateDb) {
            this.dbUpdater = new DbUpdater(crawl_path);
            dbUpdater.setTaskName(this.getTaskName());
            dbUpdater.initUpdater();
            dbUpdater.lock();

            segment_path = crawl_path + "/segments/" + SegmentWriter.createSegmengName();
            segmengwriter = new SegmentWriter(segment_path);
        }
        
        
        running=true;

    }

    public void fetchAll(Generator generator) throws IOException {
        start();
        
        fetchQueue=new FetchQueue();
        feeder=new QueueFeeder(fetchQueue,generator,1000);
        feeder.start();
        
        for(int i=0;i<threads;i++){
            FetcherThread fetcherThread=new FetcherThread();
            fetcherThread.start();
        }
        
        do{
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
        }while(activeThreads.get()>0 && running);
        
        
        end();

    }

    
    boolean running;
    public void stop() throws IOException {
        running=false;
    }



    private void end() throws IOException {
       
        if (needUpdateDb) {
            dbUpdater.closeUpdater();
            dbUpdater.merge(segment_path);
            dbUpdater.unlock();
            segmengwriter.close();
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

    public static void main(String[] args) throws IOException {
        CollectionGenerator generator = new CollectionGenerator();
        generator.addUrl("http://www.hfut.edu.cn/ch/");
        generator.addUrl("http://news.hfut.edu.cn/");
        Fetcher fetcher = new Fetcher();
        fetcher.fetchAll(generator);
        
        

    }

}
