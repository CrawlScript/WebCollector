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
import java.util.regex.Pattern;



/**
 *
 * @author hu
 */
public class StandardGenerator extends Generator {

    public String crawlPath;
    
    public String getSegmentPath(){
        String[] segment_list=new File(crawlPath,"segments").list();
        String segment_path=null;
        long max=0;
        for(String segment:segment_list){
            long timestamp=Long.valueOf(segment);
            if(timestamp>max){
                max=timestamp;
                segment_path=segment;
            }
        }
        return segment_path;
    }
    
    public StandardGenerator(String crawlPath){
        this.crawlPath=crawlPath;
        try {
            backup();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        
        DbUpdater dbupdater=new DbUpdater(crawlPath);
        
        try {
            if(dbupdater.isLocked()){
                String segment_path=getSegmentPath();
                if(segment_path!=null){
                    dbupdater.merge(segment_path);
                }
                dbupdater.unlock();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try {
            initReader();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void backup() throws IOException {
        DbUpdater.backup(crawlPath);
    }


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
    
    DbReader<CrawlDatum> dbreader;

    public void initReader() throws IOException{   
        File oldfile=new File(crawlPath, Config.old_info_path);
        dbreader=new DbReader<CrawlDatum>(CrawlDatum.class,oldfile);
    }

    public boolean shouldFilter(String url) {
        return false;
    }


}
