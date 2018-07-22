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

package cn.edu.hfut.dmic.webcollector.plugin.rocks;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.CrawlDatumFormater;
import cn.edu.hfut.dmic.webcollector.util.GsonUtils;
import com.google.gson.JsonArray;
import org.apache.commons.io.FilenameUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.UnsupportedEncodingException;

/**
 *
 * @author hu
 */
public class RocksDBUtils {


    static{
        RocksDB.loadLibrary();
    }

    public static String getFetchPath(String crawlPath){
        return new File(crawlPath, "fetch").getAbsolutePath();
//        return FilenameUtils.concat(crawlPath, "fetch");
    }

    public static String getLinkPath(String crawlPath){
        return new File(crawlPath, "link").getAbsolutePath();
//        return FilenameUtils.concat(crawlPath, "link");
    }

    public static String getCrawldbPath(String crawlPath){
        return new File(crawlPath, "crawldb").getAbsolutePath();
//        return FilenameUtils.concat(crawlPath, "crawldb");
    }

    public static RocksDB openCrawldbDatabase(String crawlPath) throws RocksDBException {
        return open(getCrawldbPath(crawlPath));
    }

    public static RocksDB openFetchDatabase(String crawlPath) throws RocksDBException {
        return open(getFetchPath(crawlPath));
    }

    public static RocksDB openLinkDatabase(String crawlPath) throws RocksDBException {
        return open(getLinkPath(crawlPath));
    }


    public static void destroyFetchDatabase(String crawlPath) throws RocksDBException {
        RocksDB.destroyDB(getFetchPath(crawlPath), createDefaultDBOptions());
    }

    public static void destroyLinkDatabase(String crawlPath) throws RocksDBException {
        RocksDB.destroyDB(getLinkPath(crawlPath), createDefaultDBOptions());
    }




    public static Options createDefaultDBOptions(){
        return new Options().setCreateIfMissing(true);
    }

    public static RocksDB open(String dbPath) throws RocksDBException {
        File dbParentDir = new File(dbPath).getParentFile();
        if(!dbParentDir.exists()){
            dbParentDir.mkdirs();
        }

        Options rocksOptions = RocksDBUtils.createDefaultDBOptions();
        return RocksDB.open(rocksOptions, dbPath);
    }
    
    public static void writeDatum(RocksDB rocksDB,CrawlDatum datum) throws Exception{
        String key = datum.key();
        String value = datum.asJsonArray().toString();
        put(rocksDB, key, value);
    }
    
    public static void put(RocksDB rocksDB, String key,String value) throws UnsupportedEncodingException, RocksDBException {
        rocksDB.put(strToKeyOrValue(key), strToKeyOrValue(value));
    }

    public static String get(RocksDB rocksDB, String key) throws UnsupportedEncodingException, RocksDBException {
        byte[] value = rocksDB.get(strToKeyOrValue(key));
        if(value == null){
            return null;
        }else {
            return keyOrValueToStr(value);
        }
    }

    public static String keyOrValueToStr(byte[] keyOrValue) throws UnsupportedEncodingException {
        return new String(keyOrValue, "utf-8");
    }
    
    public static byte[] strToKeyOrValue(String str) throws UnsupportedEncodingException{
        return str.getBytes("utf-8");
    }
    
     public static CrawlDatum createCrawlDatum(byte[] key, byte[] value) throws Exception{
        String datumKey=new String(key,"utf-8");
        String valueStr=new String(value,"utf-8");
        JsonArray datumJsonArray = GsonUtils.parse(valueStr).getAsJsonArray();
        return CrawlDatum.fromJsonArray(datumKey, datumJsonArray);
    }
}
