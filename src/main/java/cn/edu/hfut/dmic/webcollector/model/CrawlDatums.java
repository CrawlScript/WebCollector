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

import com.google.gson.JsonObject;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * 用于存储多个CrawlDatum的数据结构
 *
 * @author hu
 */
public class CrawlDatums implements Iterable<CrawlDatum>, MetaSetter<CrawlDatums> {

    protected LinkedList<CrawlDatum> dataList = new LinkedList<CrawlDatum>();

    public CrawlDatums() {
    }

    public CrawlDatums(Iterable<String> links, String type) {
        add(links, type);
    }
    
    public CrawlDatums(Iterable<String> links) {
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

    public CrawlDatums add(String url, String type) {
        CrawlDatum datum = new CrawlDatum(url).type(type);
        return add(datum);
    }

    public CrawlDatums add(String url) {
        CrawlDatum datum = new CrawlDatum(url);
        return add(datum);
    }

    public CrawlDatums add(CrawlDatums datums) {
        dataList.addAll(datums.dataList);
        return this;
    }

    public CrawlDatums add(Iterable<String> links, String type) {
        for (String link : links) {
            add(link, type);
        }
        return this;
    }

    public CrawlDatums add(Iterable<String> links) {
        for (String link : links) {
            add(link);
        }
        return this;
    }

    public CrawlDatum addAndReturn(String url){
        CrawlDatum datum = new CrawlDatum(url);
        add(datum);
        return datum;
    }

    public CrawlDatums addAndReturn(Iterable<String> links){
        CrawlDatums datums = new CrawlDatums(links);
        add(datums);
        return datums;
    }

    public CrawlDatum addAndReturn(CrawlDatum datum){
        add(datum);
        return datum;
    }

    public CrawlDatums addAndReturn(CrawlDatums datums){
        add(datums);
        return datums;
    }

//    public CrawlDatums addWithKey(String key, CrawlDatum datum){
//        datum.key(key);
//        return add(datum);
//    }
//
//    public CrawlDatums addWithKey(String key, CrawlDatum datum, String type){
//        datum.key(key).type(type);
//        return add(datum);
//    }

    @Override
    public CrawlDatums meta(JsonObject metaData) {
        for(CrawlDatum datum:dataList){
            datum.meta(metaData);
        }
        return this;
    }

    @Override
    public CrawlDatums meta(String key, String value) {
        for (CrawlDatum datum : dataList) {
            datum.meta(key, value);
        }
        return this;
    }

    @Override
    public CrawlDatums meta(String key, int value) {
        for (CrawlDatum datum : dataList) {
            datum.meta(key, value);
        }
        return this;
    }

    @Override
    public CrawlDatums meta(String key, boolean value) {
        for (CrawlDatum datum : dataList) {
            datum.meta(key, value);
        }
        return this;
    }

    @Override
    public CrawlDatums meta(String key, double value) {
        for (CrawlDatum datum : dataList) {
            datum.meta(key, value);
        }
        return this;
    }

    @Override
    public CrawlDatums meta(String key, long value) {
        for (CrawlDatum datum : dataList) {
            datum.meta(key, value);
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

    public CrawlDatums type(String type) {
        for (CrawlDatum datum : this) {
            datum.type(type);
        }
        return this;
    }

    @Override
    public String toString() {
        return dataList.toString();
    }

}
