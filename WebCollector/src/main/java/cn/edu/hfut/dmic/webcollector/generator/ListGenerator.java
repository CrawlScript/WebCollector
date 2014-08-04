/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.generator;

import cn.edu.hfut.dmic.webcollector.filter.IntervalFilter;
import cn.edu.hfut.dmic.webcollector.filter.UniqueFilter;
import cn.edu.hfut.dmic.webcollector.handler.Handler;
import cn.edu.hfut.dmic.webcollector.handler.Message;
import cn.edu.hfut.dmic.webcollector.model.AvroModel;
import cn.edu.hfut.dmic.webcollector.model.Link;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.model.WritablePage;
import cn.edu.hfut.dmic.webcollector.parser.HtmlParser;
import cn.edu.hfut.dmic.webcollector.parser.ParseResult;
import cn.edu.hfut.dmic.webcollector.task.WorkQueue;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import cn.edu.hfut.dmic.webcollector.util.HttpUtils;
import cn.edu.hfut.dmic.webcollector.util.Log;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Pattern;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author hu
 */
public class ListGenerator extends Generator{

    public static final int FETCH_SUCCESS=1;
    public static final int FETCH_FAILED=2;
    public String crawl_path;



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
        DatumWriter<WritablePage> datumWriter = new ReflectDatumWriter<WritablePage>(WritablePage.class);
        DataFileWriter<WritablePage> dataFileWriter = new DataFileWriter<WritablePage>(datumWriter);
        
        dataFileWriter.create(AvroModel.getPageSchema(), currentfile);
        for(WritablePage page_record:old_records){
            dataFileWriter.append(page_record);
        }
        dataFileWriter.close();
    }
    
    public ArrayList<WritablePage> old_records=new ArrayList<WritablePage>();
   
  
    public int oldlength;

    UniqueFilter uniquefilter = new UniqueFilter();

    public void readOldinfo() throws IOException {
        
        File oldfile=new File(crawl_path, Config.old_info_path);
        DatumReader<WritablePage> datumReader = new ReflectDatumReader<WritablePage>(WritablePage.class);
        DataFileReader<WritablePage> dataFileReader = new DataFileReader<WritablePage>(oldfile, datumReader);
     
        for(WritablePage page:dataFileReader){
             old_records.add(page);
            //System.out.println(page);
            String url=page.url;
            uniquefilter.addUrl(url);
        }
       
        dataFileReader.close();
        oldlength=old_records.size(); 
    }

    public static void main(String[] args) throws IOException {
        Document doc=Jsoup.connect("http://www.mogujie.com/").get();
        Elements links=doc.select("a[href]");
        ArrayList<String> linklist=new ArrayList<String>();
        for(Element link:links){
            linklist.add(link.attr("href"));
        }
        Injector inject=new Injector("/home/hu/data/crawl_avro");
        inject.inject(linklist);
        String crawl_path = "/home/hu/data/crawl_avro";
        Handler handler=new Handler(){

            @Override
            public void handleMessage(Message msg) {
                Page page=(Page)msg.obj;
                System.out.println(page.url);
            }
            
        };
        ListGenerator bg = new ListGenerator(handler);

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

    public ListGenerator(Handler handler) {
        super(handler);
    }

    public void stop() {
        workqueue.killALl();
    }

    int threads = 10;

    public boolean shouldFilter(String url) {
        return false;
    }

    class BreadthRunnable implements Runnable {

        WritablePage page_record;
       

        public BreadthRunnable(WritablePage page_record) {
            this.page_record=page_record;
        }

        @Override
        public void run() {
     
            if (page_record.status == Page.FETCHED) {
                if(Config.interval==-1){
                    return;
                }
                
                if(page_record.fetchtime!=-1){
                    Long lasttime=page_record.fetchtime;
                    
                    IntervalFilter intervalfilter=new IntervalFilter();
                    if(intervalfilter.shouldFilter(lasttime)){
                        return;
                    }else{
                    }
 
                }
            }
          
            Page page = new Page();
            page.url = page_record.url;
            try{
                page = HttpUtils.fetchHttpResponse(page.url, conconfig, 3);
            }catch(Exception ex){
                Log.Errors("failed ",page.url);
                Message msg = new Message();
                msg.what=BreadthGenerator.FETCH_FAILED;
                msg.obj = page;
                handler.sendMessage(msg);
                return;
            }
            
            
            if(page==null){
                 Log.Errors("failed ",page.url);
                Message msg = new Message();
                msg.what=BreadthGenerator.FETCH_FAILED;
                msg.obj = page;
                handler.sendMessage(msg);
                return;
            }
            
            
            page_record.status=Page.FETCHED;

            page.fetchtime = System.currentTimeMillis();
            page_record.fetchtime=page.fetchtime;
            
            //Log.Info("fetch",taskname+":"+page.url);
            Log.Infos("fetch",page.url);

            try {
                if (page.headers.containsKey("Content-Type")) {
                    String contenttype = page.headers.get("Content-Type").toString();
                    
                    if (contenttype.contains("text/html")) {

                        
                        HtmlParser htmlparser=new HtmlParser();
                        ParseResult parseresult=htmlparser.getParse(page);
                        
                    } else {
                        //System.out.println(page.headers.get("Content-Type"));
                    }

                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            Message msg = new Message();
            msg.what=BreadthGenerator.FETCH_SUCCESS;
            msg.obj = page;
            handler.sendMessage(msg);

        }

    }
    
    public static SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");
    public static synchronized String getSegmentName(){
        try{
            Thread.sleep(1000);
        }catch(Exception ex){
            ex.printStackTrace();
        }
        return sdf.format(new Date(System.currentTimeMillis()));
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

        for (int i = 0; i < oldlength; i++) {
            WritablePage page_record=old_records.get(i);
            BreadthRunnable breathrunnable = new BreadthRunnable(page_record);
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
