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

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.BerkeleyDBUtils;
import cn.edu.hfut.dmic.webcollector.util.Config;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.io.UnsupportedEncodingException;

/**
 *
 * @author hu
 */
public class StandardGenerator implements Generator {

    Cursor cursor = null;
    Database crawldbDatabase=null;
    Environment env;
    protected int totalGenerate=0;
    protected Integer topN=null;
    protected int maxRetry=Config.MAX_RETRY;
    public StandardGenerator(Environment env) {
        this.env=env;
        totalGenerate=0;
    }
    
    public void close(){
        cursor.close();
        crawldbDatabase.close();
    }

    public DatabaseEntry key = new DatabaseEntry();
    public DatabaseEntry value = new DatabaseEntry();

    @Override
    public CrawlDatum next() {
        if(topN!=null){
            if(totalGenerate>=topN){
                return null;
            }
        }
        if(cursor==null){
            crawldbDatabase = env.openDatabase(null, "crawldb", BerkeleyDBUtils.defaultDBConfig);
            cursor = crawldbDatabase.openCursor(null, CursorConfig.DEFAULT);
        }
        while (true) {
            if (cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                try {
                    CrawlDatum datum = new CrawlDatum(key, value);
                    if (datum.getStatus() == CrawlDatum.STATUS_DB_FETCHED) {
                        continue;
                    } else {
                        if (datum.getRetry() >= maxRetry) {
                            continue;
                        }
                        totalGenerate++;
                        return datum;
                    }
                } catch (UnsupportedEncodingException ex) {
                    continue;
                }
            } else {
                return null;
            }
        }
    }

    public int getTotalGenerate() {
        return totalGenerate;
    }

    public Integer getTopN() {
        return topN;
    }

    public void setTopN(Integer topN) {
        this.topN = topN;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public void setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
    }
    
    

  
    
    

}
