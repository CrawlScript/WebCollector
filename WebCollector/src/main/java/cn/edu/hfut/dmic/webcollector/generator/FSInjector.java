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
 * 广度遍历的种子注入器
 * @author hu
 */
public class FSInjector extends BasicInjector{
    
    private String crawlPath;

    /**
     * 构造一个向指定爬取信息文件夹中注入种子的注入器
     * @param crawlPath 爬取信息文件夹
     */
    public FSInjector(String crawlPath){
        this.crawlPath=crawlPath;
    }
    
    
    
    private boolean hasInjected(){
        String infoPath=Config.current_info_path;
        File inject_file=new File(crawlPath,infoPath);
        return inject_file.exists();
    }
    
    /**
     * 以新建/追加方式注入种子url列表
     * @param urls 种子url列表
     * @param append 是否追加
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    public void inject(ArrayList<String> urls,boolean append) throws IOException{
         
        
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
    
    /*
    public static void main(String[] args) throws IOException{
        Injector inject=new Injector("/home/hu/data/crawl_avro");
        inject.inject("http://www.xinhuanet.com/");
    }
    */
}
