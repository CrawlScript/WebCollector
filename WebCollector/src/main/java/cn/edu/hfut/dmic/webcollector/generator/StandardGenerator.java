/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.generator;


import cn.edu.hfut.dmic.webcollector.handler.Handler;
import cn.edu.hfut.dmic.webcollector.handler.Message;
import cn.edu.hfut.dmic.webcollector.model.AvroModel;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.parser.HtmlParser;
import cn.edu.hfut.dmic.webcollector.model.Link;
import cn.edu.hfut.dmic.webcollector.parser.LinkUtils;
import cn.edu.hfut.dmic.webcollector.parser.ParseResult;
import cn.edu.hfut.dmic.webcollector.util.WorkQueue;
import cn.edu.hfut.dmic.webcollector.util.CharsetDetector;
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
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.jsoup.Jsoup;

/**
 *
 * @author hu
 */
public class StandardGenerator extends Generator {

    public String crawl_path;
    public StandardGenerator(String crawl_path){
        this.crawl_path=crawl_path;
        try {
            backup();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        
        DbUpdater dbupdater=new DbUpdater(crawl_path);
        try {
            if(dbupdater.isLocked()){
                dbupdater.merge();
                dbupdater.unlock();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try {
            initReader();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void backup() throws IOException {

        File oldfile = new File(crawl_path, Config.old_info_path);
        File currentfile = new File(crawl_path, Config.current_info_path);
        FileUtils.copy(currentfile, oldfile);
    }


    public static void main(String[] args) throws IOException {
        Injector inject=new Injector("/home/hu/data/crawl_avro");
        inject.inject("http://www.xinhuanet.com/");
        String crawl_path = "/home/hu/data/crawl_avro";
        StandardGenerator bg = new StandardGenerator(null) {

            @Override
            public boolean shouldFilter(String url) {
                if (Pattern.matches("http://news.xinhuanet.com/world/.*", url)) {
                    return false;
                } else {
                    return true;
                }
            }

        };
     
       

    }

  
    
    @Override
    public CrawlDatum next(){
        if(!iterator.hasNext())
            return null;

        CrawlDatum crawldatum=iterator.next();
        
        if(crawldatum==null){
            return null;
        }
        
        
        if(shouldFilter(crawldatum.url)){
            return next();
        }
        
       
   
        return crawldatum;
    }
    
    
    Iterator<CrawlDatum> iterator;
    public void initReader() throws IOException{
         
        File oldfile=new File(crawl_path, Config.old_info_path);
        DatumReader<CrawlDatum> datumReader = new ReflectDatumReader<CrawlDatum>(CrawlDatum.class);
        DataFileReader<CrawlDatum> dataFileReader = new DataFileReader<CrawlDatum>(oldfile, datumReader);
        iterator=dataFileReader.iterator();
        
        
        
    }

    public boolean shouldFilter(String url) {
        return false;
    }


}
