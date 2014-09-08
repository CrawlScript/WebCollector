/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.generator.filter;

import cn.edu.hfut.dmic.webcollector.generator.Generator;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.Config;

/**
 *
 * @author hu
 */
public class IntervalFilter extends Filter{

    public IntervalFilter(Generator generator) {
        super(generator);
    }

    @Override
    public CrawlDatum next() {
        while(true){
        CrawlDatum crawldatum=generator.next();
        
         if(crawldatum==null){
            return null;
        }
         
        
        if(crawldatum.getStatus()==CrawlDatum.STATUS_DB_UNFETCHED){
            return crawldatum;
        }
        if(Config.interval==-1){
            continue;
        }
       
        Long lasttime=crawldatum.getFetchTime();
        if(lasttime+Config.interval>System.currentTimeMillis()){
            continue;
        }
        return crawldatum;
        }
    }

   
    
}
