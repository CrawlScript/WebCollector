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
package cn.edu.hfut.dmic.webcollector.generator;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.BerkeleyDBUtils;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

/**
 *
 * @author hu
 */
public class Injector {

    Environment env;

    public Injector(Environment env) {
        this.env=env;
    }

    public void inject(String seed) throws UnsupportedEncodingException {
        Database database=env.openDatabase(null, "crawldb", BerkeleyDBUtils.defaultDBConfig);
        CrawlDatum datum = new CrawlDatum(seed, CrawlDatum.STATUS_DB_INJECTED);
        database.put(null, datum.getKey(), datum.getValue());
        database.sync();
        database.close();
    }

    public void inject(ArrayList<String> seeds) throws UnsupportedEncodingException {
        DatabaseConfig databaseConfig=new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setDeferredWrite(true);
        Database database=env.openDatabase(null, "crawldb", databaseConfig);
        for (String seed : seeds) {
            CrawlDatum datum = new CrawlDatum(seed, CrawlDatum.STATUS_DB_INJECTED);
            database.put(null, datum.getKey(), datum.getValue());
        }
        database.sync();
        database.close();
    }
}
