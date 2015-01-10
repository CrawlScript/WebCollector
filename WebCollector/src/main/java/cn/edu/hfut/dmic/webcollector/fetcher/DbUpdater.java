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
package cn.edu.hfut.dmic.webcollector.fetcher;

import cn.edu.hfut.dmic.webcollector.fetcher.SegmentWriter;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.BerkeleyDBUtils;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 用于更新爬取任务列表的类
 *
 * @author hu
 */
public class DbUpdater {
    
    public static final Logger LOG = LoggerFactory.getLogger(DbUpdater.class);

    Environment env;
    SegmentWriter segmentWriter;
    Database lockDatabase;

    public DbUpdater(Environment env) {
        this.env = env;
        segmentWriter = new SegmentWriter(env);

    }
    
    

    public void lock() throws Exception {
        lockDatabase = env.openDatabase(null, "lock", BerkeleyDBUtils.defaultDBConfig);
        DatabaseEntry key = new DatabaseEntry("lock".getBytes("utf-8"));
        DatabaseEntry value = new DatabaseEntry("locked".getBytes("utf-8"));
        lockDatabase.put(null, key, value);
        lockDatabase.sync();
        lockDatabase.close();
    }

    public boolean isLocked() throws Exception {
        boolean isLocked = false;
        lockDatabase = env.openDatabase(null, "lock", BerkeleyDBUtils.defaultDBConfig);
        DatabaseEntry key = new DatabaseEntry("lock".getBytes("utf-8"));
        DatabaseEntry value = new DatabaseEntry();
        if (lockDatabase.get(null, key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            String lockInfo = new String(value.getData(), "utf-8");
            if (lockInfo.equals("locked")) {
                isLocked = true;
            }
        }
        lockDatabase.close();
        return isLocked;
    }

    public void unlock() throws Exception {

        lockDatabase = env.openDatabase(null, "lock", BerkeleyDBUtils.defaultDBConfig);
        DatabaseEntry key = new DatabaseEntry("lock".getBytes("utf-8"));
        DatabaseEntry value = new DatabaseEntry("unlocked".getBytes("utf-8"));
        lockDatabase.put(null, key, value);
        lockDatabase.sync();
        lockDatabase.close();

    }

    public void close() throws Exception {
        segmentWriter.close();
    }

    public void merge() throws Exception {
        LOG.info("start merge");
        Database crawldbDatabase = env.openDatabase(null, "crawldb", BerkeleyDBUtils.defaultDBConfig);
        /*合并fetch库*/
        LOG.info("merge fetch database");
        Database fetchDatabase = env.openDatabase(null, "fetch", BerkeleyDBUtils.defaultDBConfig);
        Cursor fetchCursor = fetchDatabase.openCursor(null, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        while(fetchCursor.getNext(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
            crawldbDatabase.put(null, key, value);
        }
        fetchCursor.close();
        fetchDatabase.close();        
        /*合并link库*/
        LOG.info("merge link database");
        Database linkDatabase = env.openDatabase(null, "link", BerkeleyDBUtils.defaultDBConfig);
        Cursor linkCursor=linkDatabase.openCursor(null,null);
        while(linkCursor.getNext(key, value,LockMode.DEFAULT)==OperationStatus.SUCCESS){
            if(!(crawldbDatabase.get(null, key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS)){
                crawldbDatabase.put(null, key, value);
            }
        }
        linkCursor.close();
        linkDatabase.close();
        LOG.info("end merge");
        crawldbDatabase.sync();
        crawldbDatabase.close();

        
        env.removeDatabase(null, "fetch");
        LOG.debug("remove fetch database");
        env.removeDatabase(null, "link");
        LOG.debug("remove link database");
  
    }

    public SegmentWriter getSegmentWriter() {
        return segmentWriter;
    }

}
