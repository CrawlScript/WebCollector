/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.generator;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import org.json.JSONArray;
import org.json.JSONObject;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;

/**
 *
 * @author hu
 */
public class Injector {
    
    String crawl_path;
    public Injector(String crawl_path){
        this.crawl_path=crawl_path;
    }
    
    public void inject(String url) throws IOException{
        ArrayList<String> urls=new ArrayList<>();
        urls.add(url);
        inject(urls);
    }
    
    
    public void inject(ArrayList<String> urls) throws UnsupportedEncodingException, IOException{
        JSONArray ja=new JSONArray();
        for(String url:urls){
            JSONObject jo=new JSONObject();
            jo.put("url",url);
            jo.put("status", Page.UNFETCHED);
            ja.put(jo);
        }
        byte[] content=ja.toString().getBytes("utf-8");
        String info_path=Config.current_info_path;
        File inject_file=new File(crawl_path,info_path);
        FileUtils.writeFileWithParent(inject_file, content);
        
    }
    
    
    
    public static void main(String[] args) throws IOException{
        Injector inject=new Injector("/home/hu/data/crawl_test");
        inject.inject("http://www.xinhuanet.com/");
    }
}
