/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.webcollector.crawler;

import java.io.IOException;
import java.util.regex.Pattern;
import org.webcollector.generator.BreadthGenerator;
import org.webcollector.generator.Injector;
import org.webcollector.handler.Handler;
import org.webcollector.handler.Message;
import org.webcollector.model.Page;
import org.webcollector.output.FileSystemOutput;
import org.webcollector.util.Log;

/**
 *
 * @author hu
 */
public class BreadthCrawler {
    
    public static void main(String[] args) throws IOException{
        String crawl_path="/home/hu/data/crawl_vegnet";
        final String root="/home/hu/data/hfut";
        Injector injector=new Injector(crawl_path);
        injector.inject("http://news.hfut.edu.cn/");
        Handler gene_handler = new Handler() {
            @Override
            public void handleMessage(Message msg) {
                Page page = (Page) msg.obj;
                FileSystemOutput fsoutput = new FileSystemOutput(root);
                Log.Info("visiting " + page.url);
                fsoutput.output(page);
            }
        };
        BreadthGenerator bg=new BreadthGenerator(gene_handler){

            @Override
            public boolean shouldFilter(Page page) {
               if(Pattern.matches("http://news.hfut.edu.cn/.*", page.url))
                   return false;
               else
                   return true;
            }
        
        };
       // bg.topN=10;
        bg.setThreads(10);
        int depth=5;
        for(int i=0;i<depth;i++){
            Log.Info("depth "+(i+1)+" start...");
            bg.run(crawl_path);
            Log.Info("depth "+(i+1)+" finish...");
        }
    }
    
}
