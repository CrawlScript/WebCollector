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
package cn.edu.hfut.dmic.webcollector.crawldb;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Links;

/**
 *
 * @author hu
 */
public abstract class DBManager implements Injector, SegmentWriter{

    public abstract boolean isDBExists();

    public abstract void clear() throws Exception;

    public abstract Generator getGenerator();

    public abstract void open() throws Exception;

    public abstract void close() throws Exception;

    public abstract void inject(CrawlDatum datum, boolean force) throws Exception;

    public abstract void inject(CrawlDatums datums, boolean force) throws Exception;

    public abstract void merge() throws Exception;

    public void inject(CrawlDatum datum) throws Exception {
        inject(datum, false);
    }

//    public void inject(CrawlDatums datums, boolean force) throws Exception {
//        for (CrawlDatum datum : datums) {
//            inject(datum, force);
//        }
//    }

    public void inject(CrawlDatums datums) throws Exception {
        inject(datums, false);
    }

    public void inject(Links links, boolean force) throws Exception {
        CrawlDatums datums = new CrawlDatums(links);
        inject(datums, force);
    }

    public void inject(Links links) throws Exception {
        inject(links, false);
    }

    public void inject(String url, boolean force) throws Exception {
        CrawlDatum datum = new CrawlDatum(url);
        inject(datum, force);
    }

    public void inject(String url) throws Exception {
        CrawlDatum datum = new CrawlDatum(url);
        inject(datum);
    }

}
