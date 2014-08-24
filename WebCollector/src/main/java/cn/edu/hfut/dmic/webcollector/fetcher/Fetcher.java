/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.fetcher;

import cn.edu.hfut.dmic.webcollector.generator.CollectionGenerator;
import cn.edu.hfut.dmic.webcollector.generator.StandardGenerator;
import cn.edu.hfut.dmic.webcollector.generator.DbUpdater;
import cn.edu.hfut.dmic.webcollector.generator.Generator;
import cn.edu.hfut.dmic.webcollector.handler.Handler;
import cn.edu.hfut.dmic.webcollector.handler.Message;
import cn.edu.hfut.dmic.webcollector.model.AvroModel;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Link;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.parser.HtmlParser;
import cn.edu.hfut.dmic.webcollector.parser.ParseResult;
import cn.edu.hfut.dmic.webcollector.util.WorkQueue;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import cn.edu.hfut.dmic.webcollector.util.HandlerUtils;
import cn.edu.hfut.dmic.webcollector.util.HttpUtils;
import cn.edu.hfut.dmic.webcollector.util.Log;
import cn.edu.hfut.dmic.webcollector.util.Task;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;

/**
 *
 * @author hu
 */
public class Fetcher extends Task {
    
    public int retry=3;
    

    public static final int FETCH_SUCCESS = 1;
    public static final int FETCH_FAILED = 2;

    int threads = 10;
    String crawl_path;

    public Fetcher(String crawl_path) {
        this.crawl_path = crawl_path;
    }

    public boolean needUpdateDb=true;
    public Fetcher() {
        needUpdateDb=false;
    }
    
    public DbUpdater dbUpdater = null;


    private void start() throws IOException {
        if (needUpdateDb) {
            this.dbUpdater = new DbUpdater(crawl_path);
            dbUpdater.initUpdater();
            dbUpdater.lock();
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
            dbUpdater.merge();
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
            dbUpdater.merge();
            dbUpdater.unlock();
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
           
            response = HttpUtils.fetchHttpResponse(url, conconfig, retry);         

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
                    dbUpdater.append(crawldatum);
                    if (page.headers.containsKey("Content-Type")) {
                        String contenttype = page.headers.get("Content-Type").toString();

                        if (contenttype.contains("text/html")) {

                            HtmlParser htmlparser = new HtmlParser(Config.topN);
                            ParseResult parseresult = htmlparser.getParse(page);
                            ArrayList<Link> links = parseresult.links;

                            for (Link link : links) {
                                CrawlDatum link_crawldatum = new CrawlDatum();
                                link_crawldatum.url = link.url;
                                link_crawldatum.status = Page.UNFETCHED;
                                dbUpdater.append(link_crawldatum);
                            }

                        } else {
                            //System.out.println(page.headers.get("Content-Type"));
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
    
    

    
    public static void main(String[] args) throws IOException{
        CollectionGenerator generator=new CollectionGenerator();
        generator.addUrl("http://www.hfut.edu.cn/ch/");
        generator.addUrl("http://news.hfut.edu.cn/");
        Fetcher fetcher=new Fetcher();
        fetcher.fetchAll(generator);
        
    }
    
    

}
