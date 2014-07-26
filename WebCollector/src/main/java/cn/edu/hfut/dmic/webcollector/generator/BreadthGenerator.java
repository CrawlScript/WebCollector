/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.generator;

import cn.edu.hfut.dmic.webcollector.filter.UniqueFilter;
import cn.edu.hfut.dmic.webcollector.handler.Handler;
import cn.edu.hfut.dmic.webcollector.handler.Message;
import cn.edu.hfut.dmic.webcollector.model.AvroModel;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.parser.LinkParser;
import cn.edu.hfut.dmic.webcollector.task.WorkQueue;
import cn.edu.hfut.dmic.webcollector.util.CharsetDetector;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import cn.edu.hfut.dmic.webcollector.util.HttpUtils;
import cn.edu.hfut.dmic.webcollector.util.Log;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;


import org.jsoup.Jsoup;

/**
 *
 * @author hu
 */
public class BreadthGenerator extends Generator {

    public String crawl_path;
    public Integer topN = null;
    Schema schema=AvroModel.getPageSchema();
  
    
    
    public void backup() throws IOException {

        File oldfile = new File(crawl_path, Config.old_info_path);
        File currentfile = new File(crawl_path, Config.current_info_path);
        FileUtils.copy(currentfile, oldfile);
    }

    public void update() throws UnsupportedEncodingException, IOException {
        File currentfile = new File(crawl_path, Config.current_info_path);
        if(!currentfile.getParentFile().exists()){
            currentfile.getParentFile().mkdirs();
        }
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        
        dataFileWriter.create(schema, currentfile);
        for(GenericRecord page_record:old_records){
            dataFileWriter.append(page_record);
        }
        
        dataFileWriter.close();

    }
    public ArrayList<GenericRecord> old_records=new ArrayList<GenericRecord>();
   
  
    public int oldlength;

    UniqueFilter uniquefilter = new UniqueFilter();

    public void readOldinfo() throws IOException {
        
        File oldfile=new File(crawl_path, Config.old_info_path);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(oldfile, datumReader);
        GenericRecord page = null;
        while (dataFileReader.hasNext()) {

            page = dataFileReader.next(page);
            old_records.add(page);
            System.out.println(page);
            String url=page.get("url").toString();
            uniquefilter.addUrl(url);
        }
        dataFileReader.close();
        oldlength=old_records.size();

        
    }

    public static void main(String[] args) throws IOException {

        String crawl_path = "/home/hu/data/crawl_avro";
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

            GenericRecord page_record=old_records.get(index);
            
            if ((int)page_record.get("status") == Page.FETCHED) {
                return;
            }
           
            Page page = new Page();
            page.url = page_record.get("url").toString();

            page = HttpUtils.fetchHttpResponse(page.url, conconfig, 3);

            page_record.put("status", Page.FETCHED);

            page.fetchtime = System.currentTimeMillis();
            page_record.put("fetchtime", page.fetchtime);
            
            Log.Info("fetch",page.url);

            try {
                if (page.headers.containsKey("Content-Type")) {
                    String contenttype = page.headers.get("Content-Type").toString();
                    
                    if (contenttype.contains("text/html")) {

                        String charset = CharsetDetector.guessEncoding(page.content);
                       
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
                            GenericRecord outlink_record=new GenericData.Record(schema);
                            outlink_record.put("url", link);
                            outlink_record.put("status", Page.UNFETCHED);
                            
                           
                            synchronized (old_records) {
                                old_records.add(outlink_record);           
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
            GenericRecord page_record=old_records.get(i);
            int status=(int)page_record.get("status");
            if(status==Page.UNFETCHED){
                if(hasUnfetched==false){
                    hasUnfetched=true;
                }
                unfetched_count++;
            }
            
        }
        Log.Info("info",unfetched_count+" pages to fetch");
        if(!hasUnfetched){
            Log.Info("info","Nothing to fetch");
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
