/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.generator;

import cn.edu.hfut.dmic.webcollector.model.AvroModel;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import java.io.File;
import java.io.IOException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;

/**
 *
 * @author hu
 */
public class DbWriter {
    
    
    
    public DataFileWriter<CrawlDatum> dataFileWriter;
    
  
    
 
    
    public DbWriter(File dbfile,boolean append) throws IOException{
        DatumWriter<CrawlDatum> datumWriter = new ReflectDatumWriter<CrawlDatum>(CrawlDatum.class);
        dataFileWriter = new DataFileWriter<CrawlDatum>(datumWriter);
        if(!append){
            dataFileWriter.create(AvroModel.getPageSchema(), dbfile);
        }else{
            dataFileWriter.appendTo(dbfile);
        }
    }
    
    public DbWriter(String dbpath,boolean append) throws IOException{
        this(new File(dbpath),append);
    }
    
    public DbWriter(String dbpath) throws IOException{
        this(dbpath,false);
    }
    
    public DbWriter(File dbfile) throws IOException{
        this(dbfile,false);
    }
    
    public void flush() throws IOException{
        dataFileWriter.flush();
    }
    
    public void write(CrawlDatum crawldatum) throws IOException{
        dataFileWriter.append(crawldatum);
    }
    
    public void close() throws IOException{
        dataFileWriter.close();
    }
    
}
