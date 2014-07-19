/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.generator;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import cn.edu.hfut.dmic.webcollector.filter.UniqueFilter;
import cn.edu.hfut.dmic.webcollector.handler.Handler;
import cn.edu.hfut.dmic.webcollector.handler.Message;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.parser.LinkParser;
import cn.edu.hfut.dmic.webcollector.task.WorkQueue;
import cn.edu.hfut.dmic.webcollector.util.CharsetDetector;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import cn.edu.hfut.dmic.webcollector.util.HttpUtils;

/**
 *
 * @author hu
 */
public class TraverseGenerator extends Generator {

    public ArrayList<String> seeds = new ArrayList<String>();
    ConnectionConfig conconfig = null;

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
    
    
    public TraverseGenerator(Handler handler) {
        super(handler);
    }

    public void stop() {
        workqueue.killALl();
    }

    public void addSeed(String url) {
        seeds.add(url);
    }

    int threads = 10;

    public boolean shouldFilter(Page page) {
        return false;
    }

    class TraverseRunnable implements Runnable {

        public String url;

        public TraverseRunnable(String url) {
            this.url = url;

        }
        UniqueFilter uniquefilter = new UniqueFilter();

        @Override
        public void run() {
            Page page = new Page();
            page.url = this.url;

            if (shouldFilter(page)) {
                return;
            }
            if (uniquefilter.shouldFilter(this.url)) {
                return;
            }

            page = HttpUtils.fetchHttpResponse(url, conconfig, 3);

            try {
                if (page.headers.containsKey("Content-Type")) {
                    if (page.headers.get("Content-Type").toString().contains("text/html")) {

                        String charset = CharsetDetector.guessEncoding(page.content);
                        System.out.println("charset=" + charset);
                        page.html = new String(page.content, charset);
                        page.doc = Jsoup.parse(page.html);
                        page.doc.setBaseUri(page.url);
                        ArrayList<String> outlinks = LinkParser.getAll(page);
                        for (String link : outlinks) {

                            workqueue.execute(new TraverseRunnable(link));

                        }

                    } else {
                        //System.out.println(page.headers.get("Content-Type"));
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            Message msg = new Message();
            msg.obj = page;
            handler.sendMessage(msg);

        }

    }

    WorkQueue workqueue;

    @Override
    public void generate() {
        if (seeds.size() == 0) {
            System.out.println("Please use TraverseGenerator.addSeed to add at least one seed");
            return;
        }
        workqueue = new WorkQueue(threads);
        for (String seed : seeds) {

            workqueue.execute(new TraverseRunnable(seed));
        }

    }

}
