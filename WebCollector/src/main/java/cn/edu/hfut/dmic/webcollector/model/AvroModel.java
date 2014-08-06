/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.model;

import cn.edu.hfut.dmic.webcollector.generator.Injector;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

/**
 *
 * @author hu
 */
public class AvroModel {
    public static Schema page_schema=null;
    public static Schema content_schema=null;
    public static Schema parse_schema=null;
    public static Schema fetch_schema=null;
    public static Schema index_schema=null;
    public static Schema getPageSchema(){
        if(page_schema==null){
            page_schema=ReflectData.get().getSchema(CrawlDatum.class);
        }
        return page_schema;
    }
   
    
    
    
}
