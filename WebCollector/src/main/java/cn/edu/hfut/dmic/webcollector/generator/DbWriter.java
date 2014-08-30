/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.generator;

import cn.edu.hfut.dmic.webcollector.model.AvroModel;
import java.io.File;
import java.io.IOException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;

/**
 *
 * @author hu
 */
public class DbWriter<T> {
    
    
    
    public DataFileWriter<T> dataFileWriter;
 
    public Class<T> type;
    public DbWriter(Class<T> type,File dbfile,boolean append) throws IOException{
        this.type=type;
        DatumWriter<T> datumWriter = new ReflectDatumWriter<T>(type);
        dataFileWriter = new DataFileWriter<T>(datumWriter);
        if(!append){
            if(!dbfile.getParentFile().exists()){
                dbfile.getParentFile().mkdirs();
            }
            dataFileWriter.create(AvroModel.getSchema(type), dbfile);
            
        }else{
            dataFileWriter.appendTo(dbfile);
        }
    }
    
    public DbWriter(Class<T> type,String dbpath,boolean append) throws IOException{
        this(type,new File(dbpath),append);
    }
    
    public DbWriter(Class<T> type,String dbpath) throws IOException{
        this(type,dbpath,false);
    }
    
    public DbWriter(Class<T> type,File dbfile) throws IOException{
        this(type,dbfile,false);
    }
    
    public void flush() throws IOException{
        dataFileWriter.flush();
    }
    
    public void write(T data) throws IOException{
        dataFileWriter.append(data);
    }
    
    public void close() throws IOException{
        dataFileWriter.close();
    }
    
}
