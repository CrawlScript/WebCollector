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

package cn.edu.hfut.dmic.webcollector.plugin.redis;

import cn.edu.hfut.dmic.webcollector.generator.Generator;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * @author hu
 */
public class RedisGenerator implements Generator{
    
   

    private RedisHelper redisHelper;
    private Iterator ite;
    public RedisGenerator(String tableName, String redisIP, int redisPort) {
       redisHelper=new RedisHelper(tableName, redisIP, redisPort);
       Set crawldbSet=redisHelper.getCrawlDb();
       ite=crawldbSet.iterator();
    }

    @Override
    public CrawlDatum next() {
        if(ite.hasNext()){
            String key=ite.next().toString();
            return redisHelper.getCrawlDatumByKey(key);
        }
        else
            return null;
    }
    
    public static void main(String[] args){
        //RedisHelper helper=new RedisHelper("test", "127.0.0.1",6379);
        //for(int i=0;i<100;i++)
        //    helper.inject("http://www.baidu.com"+i, false);
//helper.deleteTable();
        RedisGenerator g=new RedisGenerator("test", "127.0.0.1",6379);
        CrawlDatum d=null;
        while((d=g.next())!=null){
            System.out.println(d.getUrl());
        }
    }
    
}
