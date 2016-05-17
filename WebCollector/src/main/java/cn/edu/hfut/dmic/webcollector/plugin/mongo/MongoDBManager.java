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
package cn.edu.hfut.dmic.webcollector.plugin.mongo;

import cn.edu.hfut.dmic.webcollector.crawldb.Generator;
import cn.edu.hfut.dmic.webcollector.crawldb.DBManager;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.util.CrawlDatumFormater;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class MongoDBManager extends DBManager {

    Logger LOG = LoggerFactory.getLogger(MongoDBManager.class);

    String crawlID;
    MongoClient client;
    MongoDatabase database;
    MongoGenerator generator = null;

    public MongoDBManager(String crawlID, MongoClient client) {
        this.crawlID = crawlID;
        this.client = client;
        this.generator = new MongoGenerator(crawlID, client);

    }

    @Override
    public void inject(CrawlDatum datum, boolean force) throws Exception {
        MongoCollection crawldb = database.getCollection("crawldb");
        if (force) {
            MongoDBUtils.updateOrInsert(crawldb, datum);
        } else {
            MongoDBUtils.insertIfNotExists(crawldb, datum);
        }
    }

    @Override
    public void inject(CrawlDatums datums, boolean force) throws Exception {
        for (CrawlDatum datum : datums) {
            inject(datum, force);
        }
    }

    @Override
    public void open() throws Exception {
        database = client.getDatabase(crawlID);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    //public int BUFFER_SIZE = 20;
    MongoCollection fetch = null;
    MongoCollection link = null;
    MongoCollection redirect = null;

    @Override
    public void initSegmentWriter() throws Exception {
        fetch = database.getCollection("fetch");
        link = database.getCollection("link");
        redirect = database.getCollection("redirect");
    }

    @Override
    public void wrtieFetchSegment(CrawlDatum fetchDatum) throws Exception {
        Document doc = CrawlDatumFormater.datumToBson(fetchDatum);
        fetch.insertOne(doc);
    }

    @Override
    public void writeRedirectSegment(CrawlDatum datum, String realUrl) throws Exception {
        String key = datum.getKey();
        Document doc = new Document("_id", key)
                .append("realUrl", realUrl);
        MongoDBUtils.updateOrInsert(redirect, doc);
    }

    @Override
    public synchronized void wrtieParseSegment(CrawlDatums parseDatums) throws Exception {
        for (CrawlDatum datum : parseDatums) {
            MongoDBUtils.updateOrInsert(link, datum);
        }
    }

    @Override
    public void closeSegmentWriter() throws Exception {

    }

    @Override
    public void merge() throws Exception {
        MongoCollection crawldb = database.getCollection("crawldb");
        MongoCollection fetch = database.getCollection("fetch");
        MongoCollection link = database.getCollection("link");
        LOG.info("start merge");

        /*合并fetch库*/
        LOG.info("merge fetch database");
        FindIterable<Document> findIte = fetch.find();
        for (Document fetchDoc : findIte) {
            MongoDBUtils.updateOrInsert(crawldb, fetchDoc);
        }
        /*合并link库*/
        LOG.info("merge link database");
        findIte = link.find();
        for (Document linkDoc : findIte) {
            MongoDBUtils.insertIfNotExists(crawldb, linkDoc);
        }

        LOG.info("end merge");

        fetch.drop();
        LOG.debug("remove fetch database");
        link.drop();
        LOG.debug("remove link database");

    }

    @Override
    public void lock() throws Exception {
        MongoCollection lock = database.getCollection("lock");
        Document lockDoc = new Document("_id", "lock")
                .append("lock", "locked");
        MongoDBUtils.updateOrInsert(lock, lockDoc);
    }

    @Override
    public boolean isLocked() throws Exception {
        MongoCollection lock = database.getCollection("lock");
        Document idDoc = new Document("_id", "lock");
        FindIterable<Document> findIte = lock.find(idDoc);
        Document lockDoc = findIte.first();
        if (lockDoc != null && lockDoc.getString("lock").equals("locked")) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void unlock() throws Exception {
        MongoCollection lock = database.getCollection("lock");
        Document lockDoc = new Document("_id", "lock")
                .append("lock", "unlocked");
        MongoDBUtils.updateOrInsert(lock, lockDoc);
    }

    @Override
    public boolean isDBExists() {
        for (String name : client.listDatabaseNames()) {
            if (name.equals(crawlID)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void clear() throws Exception {
        database = client.getDatabase(crawlID);
        database.drop();
    }

    @Override
    public Generator getGenerator() {
        return generator;
    }

}
