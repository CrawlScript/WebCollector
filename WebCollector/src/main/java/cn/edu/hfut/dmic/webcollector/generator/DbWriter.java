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
