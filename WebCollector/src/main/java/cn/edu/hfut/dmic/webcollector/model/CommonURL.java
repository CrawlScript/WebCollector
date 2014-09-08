/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.model;

import cn.edu.hfut.dmic.webcollector.crawler.BreadthCrawler;
import cn.edu.hfut.dmic.webcollector.fetcher.Fetcher;
import cn.edu.hfut.dmic.webcollector.generator.CollectionGenerator;
import java.io.IOException;



/**
 *
 * @author hu
 */
public class CommonURL {
    public static void main(String[] args) throws IOException {
        CollectionGenerator generator = new CollectionGenerator();
        generator.addUrl("http://www.hfut.edu.cn/ch/");
        generator.addUrl("http://news.hfut.edu.cn/");
        Fetcher fetcher = new Fetcher();
        fetcher.fetchAll(generator);
        
        BreadthCrawler bc=new BreadthCrawler();
        
                
        
        

    }
    
    
    
    
    /*
    public String baseurl;
    public HashMap<String,String> params_map=new HashMap<String, String>();
    public CommonURL(String url){
        int q_index=url.indexOf("?");
        if(q_index==-1){
            baseurl=url;
            return;
        }
        baseurl=url.substring(0,q_index);
        String params_str=url.substring(q_index+1);
        String[] params = params_str.split("&");    
        for (String param : params){  
            String name = param.split("=")[0];  
            String value = param.split("=")[1];  
            params_map.put(name, value);  
        } 
      
    }
    
    public void print(){
        System.out.println("baseurl:"+baseurl);
        for(Entry<String,String> entry:params_map.entrySet()){
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
    }
    
    public static void main(String[] args){
        CommonURL commonurl=new CommonURL("http://nc.mofcom.gov.cn/channel/gxdj/jghq/jg_detail.shtml?id=20859&test=xxx");
        commonurl.print();
    }
    
    */
}
