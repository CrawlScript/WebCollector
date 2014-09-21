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
 * 写Avro文件的Writer
 * @author hu
 * @param <T> 待写入数据的数据类型
 */
public class DbWriter<T> {
    
    
    
    private DataFileWriter<T> dataFileWriter;
 
    private Class<T> type;

    /**
     * 构造一个向avro文件中写入指定类型数据的Writer
     * @param type 指定的数据类型
     * @param dbfile avro文件
     * @param append 是否追加
     * @throws IOException
     */
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
    
    /**
     * 构造一个向avro文件中写入指定类型数据的Writer
     * @param type 指定的数据类型
     * @param dbpath avro文件路径
     * @param append 是否追加
     * @throws IOException
     */
    public DbWriter(Class<T> type,String dbpath,boolean append) throws IOException{
        this(type,new File(dbpath),append);
    }
    
    /**
     * 构造一个向avro文件中以新建方式写入指定类型数据的Writer
     * @param type 指定的数据类型
     * @param dbpath avro文件路径
     * @throws IOException
     */
    public DbWriter(Class<T> type,String dbpath) throws IOException{
        this(type,dbpath,false);
    }
    
    /**
     * 构造一个向avro文件中以新建方式写入指定类型数据的Writer
     * @param type 指定的数据类型
     * @param dbfile avro文件
     * @throws IOException
     */
    public DbWriter(Class<T> type,File dbfile) throws IOException{
        this(type,dbfile,false);
    }
    
    /**
     * 刷新该Writer的缓冲
     * @throws IOException
     */
    public void flush() throws IOException{
        dataFileWriter.flush();
    }
    
    /**
     * 写入数据
     * @param data 要写入的数据
     * @throws IOException
     */
    public void write(T data) throws IOException{
        dataFileWriter.append(data);
    }
    
    /**
     * 关闭该Writer
     * @throws IOException
     */
    public void close() throws IOException{
        dataFileWriter.close();
    }
    
}
