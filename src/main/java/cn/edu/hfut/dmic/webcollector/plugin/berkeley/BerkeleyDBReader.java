/*
 * Copyright (C) 2016 hu
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
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class BerkeleyDBReader {

    public static final Logger LOG = LoggerFactory.getLogger(BerkeleyDBReader.class);
    public String crawlPath;

    Cursor cursor = null;
    Database crawldbDatabase = null;
    Environment env = null;

    public BerkeleyDBReader(String crawlPath) {
        this.crawlPath = crawlPath;
        File dir = new File(crawlPath);
        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig.setAllowCreate(true);
        env = new Environment(dir, environmentConfig);
        crawldbDatabase = env.openDatabase(null, "crawldb", BerkeleyDBUtils.defaultDBConfig);
        cursor = crawldbDatabase.openCursor(null, CursorConfig.DEFAULT);
    }

    protected DatabaseEntry key = new DatabaseEntry();
    protected DatabaseEntry value = new DatabaseEntry();

    public CrawlDatum next() throws Exception {
        if (cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            CrawlDatum datum = BerkeleyDBUtils.createCrawlDatum(key, value);
            return datum;
        } else {
            return null;
        }
    }

    public void close() {
        if (cursor != null) {
            cursor.close();
        }
        cursor = null;
        if (crawldbDatabase != null) {
            crawldbDatabase.close();
        }
        if (env != null) {
            env.close();
        }
    }
    
}
