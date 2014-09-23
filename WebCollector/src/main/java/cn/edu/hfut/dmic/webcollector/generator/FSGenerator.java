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
import java.io.File;
import java.io.IOException;



/**
 * 广度遍历使用的爬取任务生成器
 * @author hu
 */
public class FSGenerator extends Generator {

    private String crawlPath;
    private DbReader<CrawlDatum> dbreader;
    
    
    
   
    
    /**
     * 构造一个广度遍历爬取任务生成器，从制定路径的文件夹中获取任务
     * @param crawlPath 存储爬取信息的文件夹
     */
    public FSGenerator(String crawlPath){
        this.crawlPath=crawlPath;
  
        
        try {
             File oldfile=new File(crawlPath, Config.current_info_path);
        DbReader<CrawlDatum> r=new DbReader<CrawlDatum>(CrawlDatum.class,oldfile);
       
        dbreader=new DbReader<CrawlDatum>(CrawlDatum.class,oldfile);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    
  
    
    @Override
    public CrawlDatum next(){
        if(!dbreader.hasNext())
            return null;
       
        CrawlDatum crawldatum=dbreader.readNext();   

        if(crawldatum==null){
            return null;
        }

        if(shouldFilter(crawldatum.getUrl())){
            return next();
        }
        return crawldatum;
    }
    
   

   

    /**
     * 用户自定义的过滤规则，可以通过Override这个函数，来定义自己的StandardGenerator
     * @param url
     * @return 是否需要过滤这个url
     */
    protected boolean shouldFilter(String url) {
        return false;
    }
    
    
    /*
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
    */


}
