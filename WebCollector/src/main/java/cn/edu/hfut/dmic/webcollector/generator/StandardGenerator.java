/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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

    public String crawl_path;
    
    public String getSegmentPath(){
        String[] segment_list=new File(crawl_path,"segments").list();
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
    
    public StandardGenerator(String crawl_path){
        this.crawl_path=crawl_path;
        try {
            backup();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        
        DbUpdater dbupdater=new DbUpdater(crawl_path);
        dbupdater.setTaskname(this.taskname);
        
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
        DbUpdater.backup(crawl_path);
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

        if(shouldFilter(crawldatum.url)){
            return next();
        }
        return crawldatum;
    }
    
    DbReader<CrawlDatum> dbreader;

    public void initReader() throws IOException{   
        File oldfile=new File(crawl_path, Config.old_info_path);
        dbreader=new DbReader<CrawlDatum>(CrawlDatum.class,oldfile);
    }

    public boolean shouldFilter(String url) {
        return false;
    }


}
