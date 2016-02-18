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
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.CrawlDatumFormater;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import java.io.UnsupportedEncodingException;
import org.bson.Document;

/**
 *
 * @author hu
 */
public class MongoDBUtils {

    public static UpdateOptions forcedUo = new UpdateOptions().upsert(true);
    public static UpdateOptions uo = new UpdateOptions().upsert(false);

    public static void updateOrInsert(MongoCollection col, Document doc) {
        String key = doc.getString("_id");
        Document idDoc = new Document("_id", key);
        col.replaceOne(idDoc, doc, forcedUo);
    }

    public static void updateOrInsert(MongoCollection col, CrawlDatum datum) {
        Document doc = CrawlDatumFormater.datumToBson(datum);
        String key = datum.getKey();
        Document idDoc = new Document("_id", key);
        col.replaceOne(idDoc, doc, forcedUo);
    }

    public static void insertIfNotExists(MongoCollection col, CrawlDatum datum) {

        String key = datum.getKey();
        Document idDoc = new Document("_id", key);
        FindIterable findIte = col.find(idDoc);
        if (findIte.first() == null) {
            Document doc = CrawlDatumFormater.datumToBson(datum);
            col.insertOne(doc);
        }
    }

    public static void insertIfNotExists(MongoCollection col, Document doc) {
        String key = doc.getString("_id");
        Document idDoc = new Document("_id", key);
        FindIterable findIte = col.find(idDoc);
        if (findIte.first() == null) {
            col.insertOne(doc);
        }
    }

}
