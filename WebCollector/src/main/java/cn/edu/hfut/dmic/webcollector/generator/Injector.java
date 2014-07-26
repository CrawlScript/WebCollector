/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.generator;

import cn.edu.hfut.dmic.webcollector.model.AvroModel;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.Config;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

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
         Schema schema = AvroModel.getPageSchema();
        
        String info_path=Config.current_info_path;
        File inject_file=new File(crawl_path,info_path);
        if(!inject_file.getParentFile().exists()){
            inject_file.getParentFile().mkdirs();
        }
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        
        dataFileWriter.create(schema, inject_file);
        
        for(String url:urls){
            GenericRecord page = new GenericData.Record(schema);
            page.put("url", url);
            page.put("status", Page.UNFETCHED);
            dataFileWriter.append(page);
        }
        dataFileWriter.close();
        
        
        
        
        
    }
    
    
    
    public static void main(String[] args) throws IOException{
        Injector inject=new Injector("/home/hu/data/crawl_avro");
        inject.inject("http://www.xinhuanet.com/");
    }
}
