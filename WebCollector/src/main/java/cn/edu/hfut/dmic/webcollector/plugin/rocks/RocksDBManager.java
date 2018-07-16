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
package cn.edu.hfut.dmic.webcollector.plugin.rocks;

import cn.edu.hfut.dmic.webcollector.crawldb.DBManager;
import cn.edu.hfut.dmic.webcollector.crawldb.Generator;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;

import cn.edu.hfut.dmic.webcollector.util.CrawlDatumFormater;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author hu
 */
public class RocksDBManager extends DBManager {

    static{
        RocksDB.loadLibrary();
    }



    Logger LOG = LoggerFactory.getLogger(RocksDBManager.class);

    String crawlPath;
    RocksGenerator generator = null;

    public RocksDBManager(String crawlPath) {
        this.crawlPath = crawlPath;
    }

    public void list() throws Exception {
        String crawldbPath = FilenameUtils.concat(crawlPath, "crawldb");
        RocksDB crawldbDatabase = RocksDBUtils.open(crawldbPath);
        RocksIterator crawldbIterator = crawldbDatabase.newIterator();

        for(crawldbIterator.seekToFirst(); crawldbIterator.isValid(); crawldbIterator.next()){
            CrawlDatum datum = RocksDBUtils.createCrawlDatum(crawldbIterator.key(), crawldbIterator.value());
            System.out.println(CrawlDatumFormater.datumToString(datum));
        }

        crawldbDatabase.close();

    }

    @Override
    public void inject(CrawlDatum datum, boolean force) throws Exception {
        RocksDB crawldbDatabase = RocksDBUtils.openCrawldbDatabase(crawlPath);
        String key = datum.key();
        if (!force) {
            if(RocksDBUtils.get(crawldbDatabase, key) != null){
                crawldbDatabase.close();
                return;
            }
        }
        RocksDBUtils.put(crawldbDatabase, key, datum.asJsonArray().toString());
        crawldbDatabase.close();
    }

    @Override
    public void inject(CrawlDatums datums, boolean force) throws Exception {
        RocksDB crawldbDatabase = RocksDBUtils.openCrawldbDatabase(crawlPath);

        for (int i = 0; i < datums.size(); i++) {
            CrawlDatum datum = datums.get(i);
            String key = datum.key();
            if (!force) {
                if(RocksDBUtils.get(crawldbDatabase, key) != null){
                    continue;
                }
            }
            RocksDBUtils.put(crawldbDatabase, key, datum.asJsonArray().toString());
        }
        crawldbDatabase.close();
    }

    @Override
    public void open() throws Exception {
        File dir = new File(crawlPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    @Override
    public void close() throws Exception {
    }

    public int BUFFER_SIZE = 1;
    RocksDB fetchDatabase = null;
    RocksDB linkDatabase = null;

    AtomicInteger count_fetch = new AtomicInteger(0);
    AtomicInteger count_link = new AtomicInteger(0);

    @Override
    public void initSegmentWriter() throws Exception {



        fetchDatabase = RocksDBUtils.openFetchDatabase(crawlPath);
        linkDatabase = RocksDBUtils.openLinkDatabase(crawlPath);

        count_fetch = new AtomicInteger(0);
        count_link = new AtomicInteger(0);
    }

    @Override
    public void writeFetchSegment(CrawlDatum fetchDatum) throws Exception {
        RocksDBUtils.writeDatum(fetchDatabase, fetchDatum);
    }


    @Override
    public void writeParseSegment(CrawlDatums parseDatums) throws Exception {
        for (CrawlDatum datum : parseDatums) {
            RocksDBUtils.writeDatum(linkDatabase, datum);
        }
    }

    @Override
    public void closeSegmentWriter() throws Exception {
        if (fetchDatabase != null) {
            fetchDatabase.close();
            fetchDatabase = null;
        }
        if (linkDatabase != null) {
            linkDatabase.close();
            linkDatabase = null;
        }
       
    }

    @Override
    public void merge() throws Exception {
        LOG.info("start merge");
        RocksDB crawldbDatabase = RocksDBUtils.openCrawldbDatabase(crawlPath);


        /*合并fetch库*/
        LOG.info("merge fetch database");
        RocksDB fetchDatabase = RocksDBUtils.openFetchDatabase(crawlPath);
        RocksIterator fetchIterator = fetchDatabase.newIterator();
        for(fetchIterator.seekToFirst(); fetchIterator.isValid(); fetchIterator.next()){
            crawldbDatabase.put(fetchIterator.key(), fetchIterator.value());
        }
        fetchDatabase.close();

        /*合并link库*/
        LOG.info("merge link database");
        RocksDB linkDatabase = RocksDBUtils.openLinkDatabase(crawlPath);
        RocksIterator linkIterator = linkDatabase.newIterator();
        for(linkIterator.seekToFirst(); linkIterator.isValid(); linkIterator.next()){
            if(crawldbDatabase.get(linkIterator.key()) == null){
                crawldbDatabase.put(linkIterator.key(), linkIterator.value());
            }
        }
        linkDatabase.close();

        LOG.info("end merge");
        crawldbDatabase.close();



//        env.removeDatabase(null, "fetch");
        RocksDBUtils.destroyFetchDatabase(crawlPath);
        LOG.debug("remove fetch database");
//        env.removeDatabase(null, "link");
        RocksDBUtils.destroyLinkDatabase(crawlPath);
        LOG.debug("remove link database");

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

    @Override
    protected Generator createGenerator() throws Exception{
        return new RocksGenerator(crawlPath);

    }

}
