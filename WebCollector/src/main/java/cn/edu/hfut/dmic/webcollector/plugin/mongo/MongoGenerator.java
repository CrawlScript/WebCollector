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
package cn.edu.hfut.dmic.webcollector.plugin.mongo;

import cn.edu.hfut.dmic.webcollector.plugin.berkeley.*;
import cn.edu.hfut.dmic.webcollector.crawldb.Generator;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.CrawlDatumFormater;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.io.File;
import java.util.Iterator;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class MongoGenerator implements Generator {

    public static final Logger LOG = LoggerFactory.getLogger(MongoGenerator.class);

    Iterator<Document> cursor = null;
    MongoCollection crawldb = null;
    protected int totalGenerate = 0;
    protected int topN = -1;



    protected int maxExecuteCount = Config.MAX_EXECUTE_COUNT;

    String crawlID;
    MongoClient client;
    MongoDatabase database;

    public MongoGenerator(String crawlID, MongoClient client) {
        this.crawlID = crawlID;
        this.client = client;

    }

    @Override
    public void open() throws Exception {
        database = client.getDatabase(crawlID);
        crawldb = database.getCollection("crawldb");
        totalGenerate = 0;
        cursor = crawldb.find().iterator();

    }

    public void close() throws Exception {
    }

    @Override
    public CrawlDatum next() {
        if (topN >= 0) {
            if (totalGenerate >= topN) {
                return null;
            }
        }

        while (cursor.hasNext()) {
            Document doc = cursor.next();
            CrawlDatum datum = CrawlDatumFormater.bsonToDatum(doc);

            if (datum.getStatus() == CrawlDatum.STATUS_DB_SUCCESS) {
                continue;
            } else {
                if (datum.getExecuteCount() > maxExecuteCount) {
                    continue;
                }
                totalGenerate++;
                return datum;
            }

        }
        return null;
    }

    @Override
    public int getTotalGenerate() {
        return totalGenerate;
    }

    public int getTopN() {
        return topN;
    }

    @Override
    public void setTopN(int topN) {
        this.topN = topN;
    }

    public int getMaxExecuteCount() {
        return maxExecuteCount;
    }

    @Override
    public void setMaxExecuteCount(int maxExecuteCount) {
        this.maxExecuteCount = maxExecuteCount;
    }



}
