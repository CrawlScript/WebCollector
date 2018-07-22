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
package cn.edu.hfut.dmic.webcollector.util;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 * @author hu
 */
public class CrawlDatumsTest {

    @Test
    public void testAdd(){
        CrawlDatums datums = new CrawlDatums();


        String url = "http://cn.bing.com/";

        CrawlDatums returnedDatums = datums.add(url);
        assertEquals(datums, returnedDatums);


        ArrayList<String> urlList = new ArrayList<String>();
        for(int i = 0;i<10;i++){
            urlList.add("https://www.google.com/"+i);
        }

        returnedDatums = datums.add(urlList);
        assertEquals(datums, returnedDatums);


    }


    @Test
    public void testAddAndReturn(){
        CrawlDatums datums = new CrawlDatums();


        String url = "http://cn.bing.com/";

        CrawlDatum addedDatum = datums.addAndReturn(url);
        assertEquals(url,addedDatum.url());


        ArrayList<String> urlList = new ArrayList<String>();
        for(int i = 0;i<10;i++){
            urlList.add("https://www.google.com/"+i);
        }

        CrawlDatums addedDatums = datums.addAndReturn(urlList);
        assertEquals(urlList.size(),addedDatums.size());

        for(int i=0;i<urlList.size();i++){
            assertEquals(urlList.get(i), addedDatums.get(i).url());
        }

        assertEquals(1 + urlList.size(), datums.size());

    }
}
