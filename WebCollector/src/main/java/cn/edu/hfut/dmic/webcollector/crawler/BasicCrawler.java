/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.crawler;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import cn.edu.hfut.dmic.webcollector.filter.RegexFilter;
import cn.edu.hfut.dmic.webcollector.generator.TraverseGenerator;
import cn.edu.hfut.dmic.webcollector.handler.Handler;
import cn.edu.hfut.dmic.webcollector.handler.Message;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.output.FileSystemOutput;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;

/**
 *
 * @author hu
 */
public class BasicCrawler {

    TraverseGenerator generator = null;
    ArrayList<String> regexs = new ArrayList<String>();
    ArrayList<String> seeds = new ArrayList<String>();
    public String root = "download";
    public String cookie=null;
    public String useragent="Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:26.0) Gecko/20100101 Firefox/26.0";

    
    public boolean couldStart(){
        return seeds.size()!=0;
    }
    public String getRoot() {
        return root;
    }

    public void setRoot(String root) {
        this.root = root;
    }
    
    

    public void addSeed(String seed) {
        seeds.add(seed);
    }

    public void autoRegex() {
        for (String seed: seeds) {
            try {
                URL _URL = new URL(seed);
                String host = _URL.getHost();
                if (host.startsWith("www.")) {
                    host = host.substring(4);
                    host=".*"+host;
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
        if(cookie!=null){
            con.setRequestProperty("Cookie", cookie);
        }

    }

    public void addRegex(String regex) {
        regexs.add(regex);
    }

    public void visit(Page page) {
        FileSystemOutput fsoutput = new FileSystemOutput(root);
        System.out.println("visit:" + page.url);
        fsoutput.output(page);
    }

    public void start(){
        start(10);
    }
    
    public void setCookie(String cookie){
        this.cookie=cookie;
    }
    
    public void setUseragent(String useragent){
        this.useragent=useragent;
    }
    
    public void start(int threads) {

        if (regexs.size() == 0) {
            autoRegex();
        }

        conconfig = new ConnectionConfig() {
            @Override
            public void config(HttpURLConnection con) {
                super.config(con);
                configCon(con);
            }
        };

        Handler gene_handler = new Handler() {
            @Override
            public void handleMessage(Message msg) {
                Page p = (Page) msg.obj;
                visit(p);
            }
        };

        generator = new TraverseGenerator(gene_handler) {
            @Override
            public boolean shouldFilter(Page page) {
                for (String regex : regexs) {
                    RegexFilter regexfilter = new RegexFilter();
                    regexfilter.addRule(regex);
                    if (regexfilter.shouldFilter(page.url)) {
                        return true;
                    } else {
                        continue;
                    }
                }
                return false;
            }

        };
        
        for(String seed:seeds){
            generator.addSeed(seed);
        }
        generator.setConconfig(conconfig);
        generator.setThreads(threads);
        generator.generate();
    }
    
    public void stop(){
        generator.stop();
    }

    public static void main(String[] args) {

        BasicCrawler crawler = new BasicCrawler();
        crawler.addSeed("http://www.zhihu.com/");
        crawler.setRoot("/home/hu/data/xaut");
        
        crawler.start(30);

    }

}
