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

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.util.BerkeleyDBUtils;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 爬取过程中，写入爬取历史、网页Content、解析信息的Writer
 *
 * @author hu
 */
public class SegmentWriter {

    public int BUFFER_SIZE = 20;
    Database fetchDatabase = null;
    Database linkDatabase = null;
    Database redirectDatabase=null;

    AtomicInteger count_fetch = new AtomicInteger(0);
    AtomicInteger count_link = new AtomicInteger(0);
    AtomicInteger count_redirect = new AtomicInteger(0);
    Environment env;

    public SegmentWriter(Environment env) {
        this.env = env;
    }

    public void init() {
        fetchDatabase = env.openDatabase(null, "fetch", BerkeleyDBUtils.defaultDBConfig);
        linkDatabase = env.openDatabase(null, "link", BerkeleyDBUtils.defaultDBConfig);
        redirectDatabase = env.openDatabase(null, "redirect", BerkeleyDBUtils.defaultDBConfig);
        
        
        count_fetch = new AtomicInteger(0);
        count_link = new AtomicInteger(0);
        count_redirect=new AtomicInteger(0);
    }

    /**
     * 写入一条爬取历史记录
     *
     * @param fetch 爬取历史记录（爬取任务)
     */
    public void wrtieFetch(CrawlDatum fetch) throws Exception {
        DatabaseEntry key = fetch.getKey();
        DatabaseEntry value = fetch.getValue();
        fetchDatabase.put(null, key, value);
        if (count_fetch.incrementAndGet() % BUFFER_SIZE == 0) {
            fetchDatabase.sync();
        }
    }
    
    public void writeRedirect(String originUrl,String realUrl) throws Exception{
        DatabaseEntry key=new DatabaseEntry(originUrl.getBytes("utf-8"));
        DatabaseEntry value=new DatabaseEntry(realUrl.getBytes("utf-8"));
        redirectDatabase.put(null, key, value);
        if (count_redirect.incrementAndGet() % BUFFER_SIZE == 0) {
            redirectDatabase.sync();
        }
    }

    public void wrtieLinks(Links links) throws Exception {
        for (String url : links) {
            CrawlDatum datum = new CrawlDatum(url, CrawlDatum.STATUS_DB_UNFETCHED);
            DatabaseEntry key = datum.getKey();
            DatabaseEntry value = datum.getValue();
            linkDatabase.put(null, key, value);
        }

        if (count_link.incrementAndGet() % BUFFER_SIZE == 0) {
            linkDatabase.sync();
        }
    }

    /**
     * 关闭Writer

     */
    public void close() throws Exception {
        fetchDatabase.sync();
        linkDatabase.sync();
        fetchDatabase.close();
        linkDatabase.close();
        redirectDatabase.sync();
        redirectDatabase.close();

    }
}
