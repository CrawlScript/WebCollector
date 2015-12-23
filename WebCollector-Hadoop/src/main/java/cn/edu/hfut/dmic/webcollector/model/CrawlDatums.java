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
package cn.edu.hfut.dmic.webcollector.model;


import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.Writable;

/**
 * 用于存储多个CrawlDatum的数据结构
 *
 * @author hu
 */
public class CrawlDatums implements Iterable<CrawlDatum>,Writable {

    protected ArrayList<CrawlDatum> dataList = new ArrayList<CrawlDatum>();

    public CrawlDatums() {
    }

    public CrawlDatums(Links links) {
        add(links);
    }

    public CrawlDatums(CrawlDatums datums) {
        add(datums);
    }

    public CrawlDatums(Collection<CrawlDatum> datums) {
        for (CrawlDatum datum : datums) {
            this.add(datum);
        }
    }

    public CrawlDatums add(CrawlDatum datum) {
        dataList.add(datum);
        return this;
    }

    public CrawlDatums add(String url) {
        CrawlDatum datum = new CrawlDatum(url);
        return add(datum);
    }

    public CrawlDatums add(CrawlDatums datums) {
        dataList.addAll(datums.dataList);
        return this;
    }

    public CrawlDatums add(Links links) {
        for (String link : links) {
            add(link);
        }
        return this;
    }

    public CrawlDatums putMetaData(String key, String value) {
        for (CrawlDatum datum : dataList) {
            datum.putMetaData(key, value);
        }
        return this;
    }

    @Override
    public Iterator<CrawlDatum> iterator() {
        return dataList.iterator();
    }

    public CrawlDatum get(int index) {
        return dataList.get(index);
    }

    public int size() {
        return dataList.size();
    }

    public CrawlDatum remove(int index) {
        return dataList.remove(index);
    }

    public boolean remove(CrawlDatum datum) {
        return dataList.remove(datum);
    }

    public void clear() {
        dataList.clear();
    }

    public boolean isEmpty() {

        return dataList.isEmpty();
    }

    public int indexOf(CrawlDatum datum) {
        return dataList.indexOf(datum);
    }
    
     @Override
    public String toString() {
        return dataList.toString();
    }

    

    @Override
    public void write(DataOutput d) throws IOException {
        int len=size();
        d.writeInt(len);
        for(int i=0;i<len;i++){
            get(i).write(d);
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        int len=di.readInt();
        for(int i=0;i<len;i++){
            CrawlDatum datum=new CrawlDatum();
            datum.readFields(di);
            add(datum);
        }
    }

}
