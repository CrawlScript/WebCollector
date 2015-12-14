/*
 * Copyright (C) 2015 hu
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

import cn.edu.hfut.dmic.webcollector.crawldb.DBManager;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.util.CrawlDatumFormater;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class BerkeleyDBManager extends DBManager {

    Logger LOG = LoggerFactory.getLogger(BerkeleyDBManager.class);

    Environment env;
    String crawlPath;

    public BerkeleyDBManager(String crawlPath) {
        this.crawlPath = crawlPath;
    }

    @Override
    public void inject(CrawlDatum datum, boolean force) throws Exception {
        Database database = env.openDatabase(null, "crawldb", BerkeleyDBUtils.defaultDBConfig);
        DatabaseEntry key = BerkeleyDBUtils.strToEntry(datum.getKey());
        DatabaseEntry value = new DatabaseEntry();
        if (!force) {
            if (database.get(null, key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                database.close();
                return;
            }
        }
        value = BerkeleyDBUtils.strToEntry(CrawlDatumFormater.datumToJsonStr(datum));
        database.put(null, key, value);
        database.sync();
        database.close();
    }

    @Override
    public void open() throws Exception {
        File dir = new File(crawlPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig.setAllowCreate(true);

        env = new Environment(dir, environmentConfig);
    }

    @Override
    public void close() throws Exception {
        env.close();
    }

    public int BUFFER_SIZE = 20;
    Database fetchDatabase = null;
    Database linkDatabase = null;
    Database redirectDatabase = null;

    AtomicInteger count_fetch = new AtomicInteger(0);
    AtomicInteger count_link = new AtomicInteger(0);
    AtomicInteger count_redirect = new AtomicInteger(0);

    @Override
    public void initSegmentWriter() throws Exception {
        fetchDatabase = env.openDatabase(null, "fetch", BerkeleyDBUtils.defaultDBConfig);
        linkDatabase = env.openDatabase(null, "link", BerkeleyDBUtils.defaultDBConfig);
        redirectDatabase = env.openDatabase(null, "redirect", BerkeleyDBUtils.defaultDBConfig);

        count_fetch = new AtomicInteger(0);
        count_link = new AtomicInteger(0);
        count_redirect = new AtomicInteger(0);
    }

    @Override
    public void wrtieFetchSegment(CrawlDatum fetchDatum) throws Exception {
        BerkeleyDBUtils.writeDatum(fetchDatabase, fetchDatum);
        if (count_fetch.incrementAndGet() % BUFFER_SIZE == 0) {
            fetchDatabase.sync();
        }
    }

    @Override
    public void writeRedirectSegment(CrawlDatum datum, String realUrl) throws Exception {
        BerkeleyDBUtils.put(redirectDatabase, datum.getKey(), realUrl);
        if (count_redirect.incrementAndGet() % BUFFER_SIZE == 0) {
            redirectDatabase.sync();
        }
    }

    @Override
    public void wrtieParseSegment(CrawlDatums parseDatums) throws Exception {
        for (CrawlDatum datum : parseDatums) {
            BerkeleyDBUtils.writeDatum(linkDatabase, datum);
        }
        if (count_link.incrementAndGet() % BUFFER_SIZE == 0) {
            linkDatabase.sync();
        }
    }

    @Override
    public void closeSegmentWriter() throws Exception {
        if (fetchDatabase != null) {
            fetchDatabase.sync();
            fetchDatabase.close();
        }
        if (linkDatabase != null) {
            linkDatabase.sync();
            linkDatabase.close();
        }
        if (redirectDatabase != null) {
            redirectDatabase.sync();
            redirectDatabase.close();
        }
    }

    @Override
    public void merge() throws Exception {
        LOG.info("start merge");
        Database crawldbDatabase = env.openDatabase(null, "crawldb", BerkeleyDBUtils.defaultDBConfig);
        /*合并fetch库*/
        LOG.info("merge fetch database");
        Database fetchDatabase = env.openDatabase(null, "fetch", BerkeleyDBUtils.defaultDBConfig);
        Cursor fetchCursor = fetchDatabase.openCursor(null, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        while (fetchCursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            crawldbDatabase.put(null, key, value);
        }
        fetchCursor.close();
        fetchDatabase.close();
        /*合并link库*/
        LOG.info("merge link database");
        Database linkDatabase = env.openDatabase(null, "link", BerkeleyDBUtils.defaultDBConfig);
        Cursor linkCursor = linkDatabase.openCursor(null, null);
        while (linkCursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            if (!(crawldbDatabase.get(null, key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)) {
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

    Database lockDatabase;

    @Override
    public void lock() throws Exception {
        openLockDatabaseByEntry("locked");
    }

    @Override
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

    @Override
    public void unlock() throws Exception {
        openLockDatabaseByEntry("unlocked");
    }

    private void openLockDatabaseByEntry(String locked) throws UnsupportedEncodingException {
        lockDatabase = env.openDatabase(null, "lock", BerkeleyDBUtils.defaultDBConfig);
        DatabaseEntry key = new DatabaseEntry("lock".getBytes("utf-8"));
        DatabaseEntry value = new DatabaseEntry(locked.getBytes("utf-8"));
        lockDatabase.put(null, key, value);
        lockDatabase.sync();
        lockDatabase.close();
    }

    @Override
    public boolean isDBExists() {
        File dir = new File(crawlPath);
        return dir.exists();
    }

    @Override
    public void clear() throws Exception {
        File dir = new File(crawlPath);
        if (dir.exists()) {
            FileUtils.deleteDir(dir);
        }
    }

}
