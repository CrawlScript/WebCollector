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

import java.io.File;
import java.io.IOException;

import java.util.Iterator;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;

/**
 * 读Avro文件的Reader
 * @author hu
 * @param <T> 待读取数据的数据类型
 */
public class DbReader<T> {

    Class<T> type;
    Iterator<T> iterator;
    DataFileReader<T> dataFileReader;

    /**
     * 构造一个从avro文件中读取指定类型数据的Reader
     * @param type 指定的数据类型
     * @param dbfile 待读取的avro文件
     * @throws IOException
     */
    public DbReader(Class<T> type,File dbfile) throws IOException {
        this.type=type;
        DatumReader<T> datumReader = new ReflectDatumReader<T>(type);
        dataFileReader = new DataFileReader<T>(dbfile, datumReader);
        iterator = dataFileReader.iterator();
    }

    /**
     * 构造一个从avro文件中读取指定类型数据的Reader
     * @param type 指定的数据类型
     * @param dbpath 待读取的avro文件的路径
     * @throws IOException
     */
    public DbReader(Class<T> type,String dbpath) throws IOException {
        this(type,new File(dbpath));
    }

    /**
     * 读取下一条数据，在文件结束时调用该方法会出错，所以在调用readNext方法前需要使
     * 用hasNext方法来判断文件是否结束
     * @return 下一条数据
     */
    public T readNext() {
        return iterator.next();
    }

    /**
     * 判断是否已读取到avro文件结尾
     * @return 是否已读取到avro文件结尾
     */
    public boolean hasNext(){
        return iterator.hasNext();
    }
    
    /**
     * 关闭该Reader
     * @throws IOException
     */
    public void close() throws IOException {
        dataFileReader.close();
    }

    /*
    public static void main(String[] args) throws IOException{
        if(args.length==0){
            System.err.println("Usage dbpath");           
            main(new String[]{"/home/hu/data/crawl_hfut1/crawldb/current/info.avro"});
            return;
        }
        String dbpath=args[0];
        DbReader<CrawlDatum> reader=new DbReader<CrawlDatum>(CrawlDatum.class,dbpath);
        int sum=0;
        int sum_fetched=0;
        int sum_unfetched=0;
        
        
        CrawlDatum crawldatum=null;

        System.out.println("start read:");
        while(reader.hasNext()){
            crawldatum=reader.readNext();
            sum++;
            switch(crawldatum.getStatus()){
                case CrawlDatum.STATUS_DB_FETCHED:
                    sum_fetched++;
                    break;
                case CrawlDatum.STATUS_DB_UNFETCHED:
                    sum_unfetched++;
                    break;
                    
            }
            
         
        }
        reader.close();
       
        
    }
    */
}
