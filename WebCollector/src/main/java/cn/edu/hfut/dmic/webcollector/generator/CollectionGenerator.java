/*
 * Copyright (C) 2014 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package cn.edu.hfut.dmic.webcollector.generator;

import cn.edu.hfut.dmic.webcollector.generator.Generator;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * 爬取指定集合中的url列表的爬取任务生成器
 * @author hu
 */
public class CollectionGenerator extends Generator{
    private ArrayList<CrawlDatum> data=new ArrayList<CrawlDatum>();
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
            crawldatum.setUrl(url);
            crawldatum.setFetchTime(CrawlDatum.FETCHTIME_UNDEFINED);
            crawldatum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
            data.add(crawldatum);
        }
        iterator=data.iterator();
    }
    
    public void addUrl(String url){
       
        CrawlDatum crawldatum=new CrawlDatum();
        crawldatum.setUrl(url);
        crawldatum.setFetchTime(CrawlDatum.FETCHTIME_UNDEFINED);
        crawldatum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
        data.add(crawldatum);
        iterator=data.iterator();
    }
    
    public void addCrawlDatums(Collection<CrawlDatum> crawldatums){
        data.addAll(crawldatums);
        iterator=data.iterator();
    }
    
    

    private Iterator<CrawlDatum> iterator;
    @Override
    public CrawlDatum next() {
        if(iterator.hasNext()){
            return iterator.next();
        }else{
            return null;
        }
    }
    
    /*
    public static void main(String[] args){
        CollectionGenerator generator=new CollectionGenerator();
        generator.addUrl("http://abc1.com");
        generator.addUrl("http://abc2.com");
        CrawlDatum crawldatum;
        while((crawldatum=generator.next())!=null){
            System.out.println(crawldatum.getUrl());
        }
    }
    */
    
}
