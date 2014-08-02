/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.crawler;

import cn.edu.hfut.dmic.webcollector.filter.RegexFilter;
import cn.edu.hfut.dmic.webcollector.generator.BreadthGenerator;
import cn.edu.hfut.dmic.webcollector.generator.Injector;
import cn.edu.hfut.dmic.webcollector.handler.Handler;
import cn.edu.hfut.dmic.webcollector.handler.Message;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.output.FileSystemOutput;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import cn.edu.hfut.dmic.webcollector.util.Log;
import cn.edu.hfut.dmic.webcollector.util.RandomUtils;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 *
 * @author hu
 */
public class BreadthCrawler {
    
    public BreadthCrawler(){
        taskname=RandomUtils.getTimeString();
    }

    public String taskname;
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

    ConnectionConfig conconfig = null;

    public void configCon(HttpURLConnection con) {
        con.setRequestProperty("User-Agent", useragent);
        if (cookie != null) {
            con.setRequestProperty("Cookie", cookie);
        }

    }

    public void visit(Page page) {
        FileSystemOutput fsoutput = new FileSystemOutput(root);
        Log.Infos("visit",page.url);
        fsoutput.output(page);
    }
    
    public void failed(Page page){
       
    }

    public void start(int depth) throws IOException {
        if (!resumable) {
            if (seeds.size() == 0) {
                Log.Infos("error:"+"Please add at least one seed");
                return;
            }
            if (regexs.size() == 0) {
                autoRegex();
            }
        }
        inject();

        initGenerator();
        for (int i = 0; i < depth; i++) {
            Log.Infos("info","starting depth "+(i+1));
            if(generator!=null)
                generate();
            else
                break;
        }
    }
    
    

    public void stop(){
        BreadthGenerator temp=generator;
        generator=null;
        
        temp.stop();
    }
    
    public void inject() throws IOException {
        
        Injector injector = new Injector(crawl_path);
        injector.setTaskname(taskname);
        if(resumable && injector.hasInjected()){
            Log.Infos("inject","no need to inject");
            return;
        }
        for (String seed : seeds) {
            injector.inject(seed);
        }
    }

    BreadthGenerator generator=null;
    
    class CommonConnectionConfig implements ConnectionConfig{
        @Override
            public void config(HttpURLConnection con) {
               
                configCon(con);
            }
    }
    
    public void initGenerator(){
         conconfig = new CommonConnectionConfig();

        Handler gene_handler = new Handler() {
            @Override
            public void handleMessage(Message msg) {
                Page page = (Page) msg.obj;
                switch(msg.what){
                    case BreadthGenerator.FETCH_SUCCESS:
                        
                        visit(page);
                        break;
                    case BreadthGenerator.FETCH_FAILED:
                        failed(page);
                        break;
                    default:
                        break;
                       
                }
            }
        };
        generator = new BreadthGenerator(gene_handler) {

            @Override
            public boolean shouldFilter(String url) {
                
                return BreadthCrawler.this.shouldFilter(url);
            }

        };
        generator.setTaskname(taskname);
        generator.setConconfig(conconfig);
        generator.setThreads(threads);
        
    }
    public void generate() throws IOException {
 
        generator.run(crawl_path);
    }

    public boolean shouldFilter(String url) {
        RegexFilter regexfilter = new RegexFilter();
        for (String regex : regexs) {
            regexfilter.addRule(regex);
        }
        return regexfilter.shouldFilter(url);
    }

    public static void main(String[] args) throws IOException {
        String crawl_path = "/home/hu/data/crawl_hfut";
        String root = "/home/hu/data/hfut";
        
       
        
        BreadthCrawler crawler=new BreadthCrawler();
        crawler.taskname=RandomUtils.getTimeString()+"hfut";
        crawler.addSeed("http://news.hfut.edu.cn/");
        crawler.setRoot(root);
        crawler.setCrawl_path(crawl_path);
        crawler.setResumable(false);
       
        
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

}
