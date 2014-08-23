/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.webcollector.generator;

import cn.edu.hfut.dmic.webcollector.generator.Injector;
import cn.edu.hfut.dmic.webcollector.generator.StandardGenerator;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Page;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author hu
 */

public class TestStandardGenerator {
    @Test
    public void testGenerator() throws IOException{
        String crawl_path="/home/hu/data/webcollector_test";
        ArrayList<String> seeds=new ArrayList<String>();
        seeds.add("http://www.sina.com.cn/");
        seeds.add("http://www.xinhuanet.com/");
        
        Injector injector=new Injector(crawl_path);
        injector.inject(seeds);
       
        StandardGenerator generator=new StandardGenerator(crawl_path);
        CrawlDatum crawldatum=null;
        ArrayList<CrawlDatum> datums=new ArrayList<CrawlDatum>();
        while((crawldatum=generator.next())!=null){
            datums.add(crawldatum);
        }
        
        Assert.assertEquals(seeds.size(),datums.size());
        for(int i=0;i<seeds.size();i++){           
            Assert.assertEquals(-1, datums.get(i).fetchtime);
            Assert.assertEquals(Page.UNFETCHED, datums.get(i).status);
            Assert.assertEquals(seeds.get(i), datums.get(i).url);
        }
        
        System.out.println("123123");
    }
    
    
   
}
