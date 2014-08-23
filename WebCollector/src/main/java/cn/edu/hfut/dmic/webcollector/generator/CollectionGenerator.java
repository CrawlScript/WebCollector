/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.generator;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Page;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 *
 * @author hu
 */
public class CollectionGenerator extends Generator{
    public ArrayList<CrawlDatum> data=new ArrayList<CrawlDatum>();
    public  CollectionGenerator(){  
        iterator=data.iterator();
    }
    
    public  CollectionGenerator(Collection<CrawlDatum> crawldatums){
        for(CrawlDatum crawldatum:crawldatums){
            data.add(crawldatum);
        }
        iterator=data.iterator();
    }
    
    public void addUrls(Collection<String> urls){
        for(String url:urls){
            CrawlDatum crawldatum=new CrawlDatum();
            crawldatum.url=url;
            crawldatum.fetchtime=Page.FETCHTIME_UNDEFINED;
            crawldatum.status=Page.UNFETCHED;
            data.add(crawldatum);
        }
        iterator=data.iterator();
    }
    
    public void addUrl(String url){
       
        CrawlDatum crawldatum=new CrawlDatum();
        crawldatum.url=url;
        crawldatum.fetchtime=Page.FETCHTIME_UNDEFINED;
        crawldatum.status=Page.UNFETCHED;
        data.add(crawldatum);
        iterator=data.iterator();
    }
    
    public void addCrawlDatums(Collection<CrawlDatum> crawldatums){
        data.addAll(crawldatums);
        iterator=data.iterator();
    }
    
    

    Iterator<CrawlDatum> iterator;
    @Override
    public CrawlDatum next() {
        if(iterator.hasNext()){
            return iterator.next();
        }else{
            return null;
        }
    }
    
    public static void main(String[] args){
        CollectionGenerator generator=new CollectionGenerator();
        generator.addUrl("http://abc1.com");
        generator.addUrl("http://abc2.com");
        CrawlDatum crawldatum;
        while((crawldatum=generator.next())!=null){
            System.out.println(crawldatum.url);
        }
    }
    
}
