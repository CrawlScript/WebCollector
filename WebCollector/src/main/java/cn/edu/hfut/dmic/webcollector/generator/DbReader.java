/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.generator;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.URLUtils;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;

/**
 *
 * @author hu
 */
public class DbReader {

    
    Iterator<CrawlDatum> iterator;
    DataFileReader<CrawlDatum> dataFileReader;

    public DbReader(File dbfile) throws IOException {
        DatumReader<CrawlDatum> datumReader = new ReflectDatumReader<CrawlDatum>(CrawlDatum.class);
        dataFileReader = new DataFileReader<CrawlDatum>(dbfile, datumReader);
        iterator = dataFileReader.iterator();
    }

    public DbReader(String dbpath) throws IOException {
        this(new File(dbpath));
    }

   

    public CrawlDatum readNext() {
        return iterator.next();
    }

    public boolean hasNext(){
        return iterator.hasNext();
    }
    
    public void close() throws IOException {
        dataFileReader.close();
    }

    
    public static void main(String[] args) throws IOException{
        if(args.length==0){
            System.err.println("Usage dbpath");           
            main(new String[]{"/home/hu/data/crawl_hfut1/crawldb/current/info.avro"});
            return;
        }
        String dbpath=args[0];
        DbReader reader=new DbReader(dbpath);
        int sum=0;
        int sum_fetched=0;
        int sum_unfetched=0;
        
        
        CrawlDatum crawldatum=null;

        System.out.println("start read:");
        while(reader.hasNext()){
            crawldatum=reader.readNext();
            sum++;
            switch(crawldatum.status){
                case Page.FETCHED:
                    sum_fetched++;
                    break;
                case Page.UNFETCHED:
                    sum_unfetched++;
                    break;
                    
            }
            
         
        }
        reader.close();
        System.out.println("Total urls:"+sum);
        System.out.println("status "+Page.FETCHED+" (fetched):"+sum_fetched);
        System.out.println("status "+Page.UNFETCHED+" (unfetched):"+sum_unfetched);
        
        
    }
}
