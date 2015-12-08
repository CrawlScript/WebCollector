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
package cn.edu.hfut.dmic.webcollector.lazy.util;


import com.mongodb.MongoClient;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class MongoHelper {

    public static final Logger LOG = LoggerFactory.getLogger(MongoHelper.class);

    protected MongoClient client;
    protected MongoDatabase db;
    protected MongoCollection collection;

    public MongoHelper(String ip, int port, String dbName, String collectionName) {
        client = new MongoClient(ip, port);
        db = client.getDatabase(dbName);
        collection = db.getCollection(collectionName);
    }

    public void addPage(String id, String url, String refer, String html) {
        try {
            collection.insertOne(new Document("_id", id)
                    .append("url", url)
                    .append("refer", refer)
                    .append("html", html));
            LOG.info("save PAGE:" + id);
        } catch (MongoWriteException ex) {
            if (ex.getMessage().startsWith("E11000")) {
                LOG.info("ignore duplicate key:" + id);
            } else {
                LOG.info("Exception when inserting", ex);
            }
        }
    }

    public void close() {
        client.close();
    }
}
