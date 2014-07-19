/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.generator;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.util.ArrayList;

import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;

import cn.edu.hfut.dmic.webcollector.filter.UniqueFilter;
import cn.edu.hfut.dmic.webcollector.handler.Handler;
import cn.edu.hfut.dmic.webcollector.handler.Message;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.parser.LinkParser;
import cn.edu.hfut.dmic.webcollector.task.WorkQueue;
import cn.edu.hfut.dmic.webcollector.util.CharsetDetector;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import cn.edu.hfut.dmic.webcollector.util.HttpUtils;
import cn.edu.hfut.dmic.webcollector.util.Log;

/**
 *
 * @author hu
 */
public class BreadthGenerator extends Generator {

    public String crawl_path;
    public Integer topN = null;

    public void backup() throws IOException {

        File oldfile = new File(crawl_path, Config.old_info_path);
        File currentfile = new File(crawl_path, Config.current_info_path);
        FileUtils.copy(currentfile, oldfile);
    }

    public void update() throws UnsupportedEncodingException, IOException {
        File currentfile = new File(crawl_path, Config.current_info_path);

        byte[] content = oldinfo.toString().getBytes("utf-8");
        FileUtils.writeFileWithParent(currentfile, content);
    }

    public JSONArray oldinfo;
    public JSONArray currentinfo;
    public int oldlength;

    UniqueFilter uniquefilter = new UniqueFilter();

    public void readOldinfo() throws IOException {
        String oldinfostr = new String(FileUtils.readFile(new File(crawl_path, Config.old_info_path)), "utf-8");
        oldinfo = new JSONArray(oldinfostr);
        oldlength = oldinfo.length();
        for (int i = 0; i < oldlength; i++) {
            uniquefilter.addUrl(oldinfo.getJSONObject(i).getString("url"));
        }
    }

    public static void main(String[] args) throws IOException {

        String crawl_path = "/home/hu/data/crawl_test";
        BreadthGenerator bg = new BreadthGenerator(null) {

            @Override
            public boolean shouldFilter(Page page) {
                if (Pattern.matches("http://news.xinhuanet.com/world/.*", page.url)) {
                    return false;
                } else {
                    return true;
                }
            }

        };
        bg.topN = 20;
        bg.run(crawl_path);

    }

    public void run(String crawl_path) throws IOException {
        this.crawl_path = crawl_path;

        backup();
        generate();
        update();
    }

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

    public BreadthGenerator(Handler handler) {
        super(handler);
    }

    public void stop() {
        workqueue.killALl();
    }

    int threads = 10;

    public boolean shouldFilter(Page page) {
        return false;
    }

    class BreadthRunnable implements Runnable {

        int index;

        public BreadthRunnable(int index) {
            this.index = index;
        }

        @Override
        public void run() {

            JSONObject page_object = oldinfo.getJSONObject(index);
            if (page_object.getInt("status") == Page.FETCHED) {
                return;
            }

            Page page = new Page();
            page.url = page_object.getString("url");

            page = HttpUtils.fetchHttpResponse(page.url, conconfig, 3);

            page_object.put("status", Page.FETCHED);

            page.fecthtime = System.currentTimeMillis();
            page_object.put("fetchtime", page.fecthtime);
            Log.Info("Fetched:" + page.url);
            try {
                if (page.headers.containsKey("Content-Type")) {
                    String contenttype = page.headers.get("Content-Type").toString();
                    page_object.put("contenttype", contenttype);
                    if (contenttype.contains("text/html")) {

                        String charset = CharsetDetector.guessEncoding(page.content);
                        page_object.put("charset", charset);
                        page.html = new String(page.content, charset);
                        page.doc = Jsoup.parse(page.html);
                        page.doc.setBaseUri(page.url);

                        ArrayList<String> outlinks = LinkParser.getLinks(page);
                        for (int i = 0; i < outlinks.size(); i++) {
                            if (uniquefilter.shouldFilter(outlinks.get(i))) {
                                outlinks.remove(i);
                                i--;
                                continue;
                            }
                            Page tempp = new Page();
                            tempp.url = outlinks.get(i);
                            if (shouldFilter(tempp)) {
                                outlinks.remove(i);
                                i--;
                                continue;
                            }
                            uniquefilter.addUrl(outlinks.get(i));

                        }
                        int maxlink = -1;
                        if (topN == null) {
                            maxlink = outlinks.size();
                        } else {
                            maxlink = topN;
                            if (maxlink > outlinks.size()) {
                                maxlink = outlinks.size();
                            }
                        }
                        for (int i = 0; i < maxlink; i++) {
                            String link = outlinks.get(i);
                            JSONObject jo_link = new JSONObject();
                            jo_link.put("url", link);
                            jo_link.put("status", Page.UNFETCHED);
                         
                           
                            synchronized (oldinfo) {

                                oldinfo.put(jo_link);           
                            }
                            
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
        try {
            readOldinfo();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        workqueue = new WorkQueue(threads);
        
        int unfetched_count=0;
        boolean hasUnfetched=false;
        for(int i=0;i<oldlength;i++){
            JSONObject page_object=oldinfo.getJSONObject(i);
            if(page_object.getInt("status")==Page.UNFETCHED){
                if(hasUnfetched==false)
                    hasUnfetched=true;
                unfetched_count++;
                //break;
            }
        }
        Log.Info(unfetched_count+" pages to fetch");
        if(!hasUnfetched){
            Log.Info("Nothing to fetch");
            return;
        }

        for (int i = 0; i < oldlength; i++) {
            BreadthRunnable breathrunnable = new BreadthRunnable(i);
            workqueue.execute(breathrunnable);
        }

        try {
            while (workqueue.isAlive()) {
                Thread.sleep(5000);
            }
            stop();

        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }

    }

}
