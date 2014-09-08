/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.generator;


import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;

import java.io.File;
import java.io.IOException;

import java.util.Iterator;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;

/**
 *
 * @author hu
 */
public class DbReader<T> {

    Class<T> type;
    Iterator<T> iterator;
    DataFileReader<T> dataFileReader;

    public DbReader(Class<T> type,File dbfile) throws IOException {
        this.type=type;
        DatumReader<T> datumReader = new ReflectDatumReader<T>(type);
        dataFileReader = new DataFileReader<T>(dbfile, datumReader);
        iterator = dataFileReader.iterator();
    }

    public DbReader(Class<T> type,String dbpath) throws IOException {
        this(type,new File(dbpath));
    }

   

    public T readNext() {
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
        DbReader<CrawlDatum> reader=new DbReader<CrawlDatum>(CrawlDatum.class,dbpath);
        int sum=0;
        int sum_fetched=0;
        int sum_unfetched=0;
        
        
        CrawlDatum crawldatum=null;

        System.out.println("start read:");
        while(reader.hasNext()){
            crawldatum=reader.readNext();
            sum++;
            switch(crawldatum.getStatus()){
                case CrawlDatum.STATUS_DB_FETCHED:
                    sum_fetched++;
                    break;
                case CrawlDatum.STATUS_DB_UNFETCHED:
                    sum_unfetched++;
                    break;
                    
            }
            
         
        }
        reader.close();
       
        
    }
}
