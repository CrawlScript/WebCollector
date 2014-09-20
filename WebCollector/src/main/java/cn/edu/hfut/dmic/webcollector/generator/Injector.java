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


import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.LogUtils;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;



/**
 *
 * @author hu
 */
public class Injector{
    
    private String crawlPath;
    public Injector(String crawlPath){
        this.crawlPath=crawlPath;
    }
    
    public void inject(String url) throws IOException{
        inject(url,false);
    }
    
    public void inject(ArrayList<String> urls) throws IOException{
        inject(urls,false);
    }
    
    public void inject(String url,boolean append) throws IOException{
        ArrayList<String> urls=new ArrayList<>();
        urls.add(url);
        inject(urls,append);
    }
    
    public boolean hasInjected(){
        String infoPath=Config.current_info_path;
        File inject_file=new File(crawlPath,infoPath);
        return inject_file.exists();
    }
    
    
    
    public void inject(ArrayList<String> urls,boolean append) throws UnsupportedEncodingException, IOException{
         
        
        String info_path=Config.current_info_path;
        File inject_file=new File(crawlPath,info_path);
        if(!inject_file.getParentFile().exists()){
            inject_file.getParentFile().mkdirs();
        }
        DbWriter<CrawlDatum> writer;
        if(inject_file.exists())
            writer=new DbWriter<CrawlDatum>(CrawlDatum.class,inject_file,append);
        else
            writer=new DbWriter<CrawlDatum>(CrawlDatum.class,inject_file,false);
        for(String url:urls){
            CrawlDatum crawldatum=new CrawlDatum();
            crawldatum.setUrl(url);
            crawldatum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
            writer.write(crawldatum);  
            LogUtils.getLogger().info("inject "+url);
        }
        writer.close();
        
        
        
        
        
    }
    
    
    
    public static void main(String[] args) throws IOException{
        Injector inject=new Injector("/home/hu/data/crawl_avro");
        inject.inject("http://www.xinhuanet.com/");
    }
}
