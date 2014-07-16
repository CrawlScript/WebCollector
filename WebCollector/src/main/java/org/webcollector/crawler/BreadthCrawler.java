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

/**
 *
 * @author hu
 */
public class BreadthCrawler {
    
    public static void main(String[] args) throws IOException{
        String crawl_path="/home/hu/data/crawl_test";
        final String root="/home/hu/data/world";
        Injector injector=new Injector(crawl_path);
        injector.inject("http://www.xinhuanet.com/");
        Handler gene_handler = new Handler() {
            @Override
            public void handleMessage(Message msg) {
                Page page = (Page) msg.obj;
                FileSystemOutput fsoutput = new FileSystemOutput(root);
                System.out.println("visit:" + page.url);
                fsoutput.output(page);
            }
        };
        BreadthGenerator bg=new BreadthGenerator(gene_handler){

            @Override
            public boolean shouldFilter(Page page) {
               if(Pattern.matches("http://news.xinhuanet.com/world/.*", page.url))
                   return false;
               else
                   return true;
            }
        
        };
        bg.topN=10;
        int depth=3;
        for(int i=0;i<depth;i++){
            bg.run(crawl_path);
        }
    }
    
}
