/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.lazy.util;

import static cn.edu.hfut.dmic.webcollector.lazy.LazyCrawler.LOG;
import com.mongodb.DuplicateKeyException;
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
