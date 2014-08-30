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


    public static Schema getSchema(Class type){
        
        Schema schema=ReflectData.get().getSchema(type);
        
        return schema;
    }
   
    
    
    
}
