/*
 * Copyright (C) 2014 hu
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
package cn.edu.hfut.dmic.webcollector.crawler;

import cn.edu.hfut.dmic.webcollector.fetcher.Fetcher;

import cn.edu.hfut.dmic.webcollector.generator.Generator;
import cn.edu.hfut.dmic.webcollector.generator.Injector;
import cn.edu.hfut.dmic.webcollector.generator.StandardGenerator;
import cn.edu.hfut.dmic.webcollector.generator.filter.IntervalFilter;
import cn.edu.hfut.dmic.webcollector.generator.filter.URLRegexFilter;
import cn.edu.hfut.dmic.webcollector.generator.filter.UniqueFilter;
import cn.edu.hfut.dmic.webcollector.handler.Handler;
import cn.edu.hfut.dmic.webcollector.handler.Message;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.output.FileSystemOutput;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import cn.edu.hfut.dmic.webcollector.util.Log;
import cn.edu.hfut.dmic.webcollector.util.RandomUtils;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;


/**
 *The web crawler that executes a breadth-first crawling.
 * 
 * @author hu
 */
public class BreadthCrawler {
    
    /**
     *
     */
    public BreadthCrawler(){
        taskname=RandomUtils.getTimeString();
    }

    private String taskname;
    private String crawl_path = "crawl";
    private String root = "data";
    private String cookie = null;
    private String useragent = "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:26.0) Gecko/20100101 Firefox/26.0";

    private int threads=10;
    private boolean resumable;

    ArrayList<String> regexs = new ArrayList<String>();
    ArrayList<String> seeds = new ArrayList<String>();

    public void addSeed(String seed) {
        seeds.add(seed);
    }
    
    

    public void addRegex(String regex) {
        regexs.add(regex);
    }
    /*
    public void autoRegex() {
        for (String seed : seeds) {
            try {
                URL _URL = new URL(seed);
                String host = _URL.getHost();
                if (host.startsWith("www.")) {
                    host = host.substring(4);
                    host = ".*" + host;
                }
                String autoregex = _URL.getProtocol() + "://" + host + ".*";

                regexs.add(autoregex);
                System.out.println("autoregex:" + autoregex);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    */
    public ConnectionConfig conconfig = null;

    public void configCon(HttpURLConnection con) {
        con.setRequestProperty("User-Agent", useragent);
        if (cookie != null) {
            con.setRequestProperty("Cookie", cookie);
        }

    }

    protected void visit(Page page) {
        FileSystemOutput fsoutput = new FileSystemOutput(root);
        Log.Infos("visit",this.taskname,page.url);
        fsoutput.output(page);
    }
    
    protected void failed(Page page){
       
    }
    

    public final static int RUNNING=1;
    public final static int STOPED=2;
    public int status;

    /**
     * start the crawler
     * @param depth depth in bread-first search
     * @throws IOException
     */
    public void start(int depth) throws IOException {
        if (!resumable) {
            if (seeds.isEmpty()) {
                Log.Infos("error:"+"Please add at least one seed");
                return;
            }
           
        }
        if (regexs.isEmpty()) {
                Log.Infos("error:"+"Please add at least one regex rule");
                return;
        }
        inject();
        status=RUNNING;
        for (int i = 0; i < depth; i++) {
           if(status==STOPED){
               break;
           }
            Log.Infos("info","starting depth "+(i+1));
            Generator generator=getGenerator();
            fetcher=getFecther();
            fetcher.fetchAll(generator);
        }
    }
    
    Fetcher fetcher;

    /**
     *
     * @throws IOException
     */
    public void stop() throws IOException{
       fetcher.stop();
       status=STOPED;
    }
    
    private void inject() throws IOException {
        
        Injector injector = new Injector(crawl_path);     
        injector.inject(seeds,resumable);
        
    }

    class CommonConnectionConfig implements ConnectionConfig{
        @Override
            public void config(HttpURLConnection con) {               
                configCon(con);
            }
    }
    
    private Fetcher getFecther(){
        
        
         Handler fetch_handler = new Handler() {
            @Override
            public void handleMessage(Message msg) {
                Page page = (Page) msg.obj;
                switch(msg.what){
                    case Fetcher.FETCH_SUCCESS:
                        
                        visit(page);
                        break;
                    case Fetcher.FETCH_FAILED:
                        failed(page);
                        break;
                    default:
                        break;
                       
                }
            }
        };
        

        
        Fetcher fetcher=new Fetcher(crawl_path);
        fetcher.setHandler(fetch_handler);
        conconfig = new CommonConnectionConfig();
        fetcher.setTaskname(taskname);
        fetcher.setThreads(threads);
        fetcher.setConconfig(conconfig);
        return fetcher;
    }
    
    private Generator getGenerator(){

        Generator generator = new StandardGenerator(crawl_path);
        generator=new UniqueFilter(new IntervalFilter(new URLRegexFilter(generator, regexs)));
        generator.setTaskname(taskname);
        return generator;
    }
   

    

    public static void main(String[] args) throws IOException {
        String crawl_path = "/home/hu/data/crawl_hfut1";
        String root = "/home/hu/data/hfut1";       
        Config.topN=500;
        BreadthCrawler crawler=new BreadthCrawler();
        crawler.taskname=RandomUtils.getTimeString()+"hfut";
       // crawler.addSeed("http://news.hfut.edu.cn/");
        crawler.addRegex("http://news.hfut.edu.cn/.*");
        crawler.setRoot(root);
        crawler.setCrawl_path(crawl_path);
        crawler.setResumable(true);      
        crawler.start(5);
    }

    public String getUseragent() {
        return useragent;
    }

    public void setUseragent(String useragent) {
        this.useragent = useragent;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public String getCrawl_path() {
        return crawl_path;
    }

    public void setCrawl_path(String crawl_path) {
        this.crawl_path = crawl_path;
    }

    public String getCookie() {
        return cookie;
    }

    public void setCookie(String cookie) {
        this.cookie = cookie;
    }

    public String getRoot() {
        return root;
    }

    public void setRoot(String root) {
        this.root = root;
    }

    public boolean isResumable() {
        return resumable;
    }

    public void setResumable(boolean resumable) {
        this.resumable = resumable;
    }

    public ConnectionConfig getConconfig() {
        return conconfig;
    }

    public void setConconfig(ConnectionConfig conconfig) {
        this.conconfig = conconfig;
    }

    public String getTaskname() {
        return taskname;
    }

    public void setTaskname(String taskname) {
        this.taskname = taskname;
    }

    
    
}
