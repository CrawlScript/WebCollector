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
package cn.edu.hfut.dmic.webcollector.plugin.rocks;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *
 * @author hu
 */
public class RocksDBReader {

    public static final Logger LOG = LoggerFactory.getLogger(RocksDBReader.class);
    public String crawlPath;


    RocksDB crawldbDatabase = null;
    RocksIterator crawldbIterator = null;

    public RocksDBReader(String crawlPath) throws RocksDBException {
        this.crawlPath = crawlPath;
        crawldbDatabase = RocksDBUtils.openCrawldbDatabase(crawlPath);
        crawldbIterator = crawldbDatabase.newIterator();
        crawldbIterator.seekToFirst();
    }



    public CrawlDatum next() throws Exception {

        if(crawldbIterator.isValid()){
            CrawlDatum datum = RocksDBUtils.createCrawlDatum(crawldbIterator.key(), crawldbIterator.value());
            crawldbIterator.next();
            return datum;
        }else{
            return null;
        }

    }

    public void close() {
        if (crawldbDatabase != null) {
            crawldbDatabase.close();
        }
    }
    
}
