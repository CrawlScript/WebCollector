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
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.io.UnsupportedEncodingException;

/**
 *
 * @author hu
 */
public class StandardGenerator implements Generator {

    Cursor cursor = null;

    public StandardGenerator(Cursor cursor) {
        this.cursor = cursor;
    }

    public DatabaseEntry key = new DatabaseEntry();
    public DatabaseEntry value = new DatabaseEntry();

    @Override
    public CrawlDatum next() {

        while (true) {
            if (cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                try {
                    CrawlDatum datum = new CrawlDatum(key, value);
                    if (datum.getStatus() == CrawlDatum.STATUS_DB_FETCHED) {
                        continue;
                    } else {
                        if (datum.getRetry() >= 10) {
                            continue;
                        }
                        return datum;
                    }
                } catch (UnsupportedEncodingException ex) {
                    continue;
                }
            } else {
                return null;
            }
        }
    }

}
