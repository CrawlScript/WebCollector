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

package cn.edu.hfut.dmic.webcollector.plugin.berkeley;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.GsonUtils;
import com.google.gson.JsonArray;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import java.io.UnsupportedEncodingException;

/**
 *
 * @author hu
 */
public class BerkeleyDBUtils {
    public static DatabaseConfig defaultDBConfig;
    
    static{
        defaultDBConfig=createDefaultDBConfig();
    }
    public static DatabaseConfig  createDefaultDBConfig(){
        DatabaseConfig databaseConfig=new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
//        databaseConfig.setDeferredWrite(true);
        return databaseConfig;
    }
    
    public static void writeDatum(Database database,CrawlDatum datum) throws Exception{
        String key = datum.key();
        String value= datum.asJsonArray().toString();
        put(database,key,value);
    }
    
    public static void put(Database database,String key,String value) throws Exception{
        database.put(null, strToEntry(key), strToEntry(value));
    }
    
    public static DatabaseEntry strToEntry(String str) throws UnsupportedEncodingException{
        return new DatabaseEntry(str.getBytes("utf-8"));
    }
    
     public static CrawlDatum createCrawlDatum(DatabaseEntry key,DatabaseEntry value) throws Exception{
        String datumKey=new String(key.getData(),"utf-8");
        String valueStr=new String(value.getData(),"utf-8");
        JsonArray datumJsonArray = GsonUtils.parse(valueStr).getAsJsonArray();
        return CrawlDatum.fromJsonArray(datumKey, datumJsonArray);
    }
}
