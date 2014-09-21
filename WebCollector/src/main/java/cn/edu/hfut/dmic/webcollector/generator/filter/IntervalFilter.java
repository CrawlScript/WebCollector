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

package cn.edu.hfut.dmic.webcollector.generator.filter;

import cn.edu.hfut.dmic.webcollector.generator.Generator;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.Config;

/**
 * 时间间隔过滤器
 * @author hu
 */
public class IntervalFilter extends Filter{

    /**
     * 构造一个时间间隔过滤器
     * @param generator 嵌套的任务生成器
     */
    public IntervalFilter(Generator generator) {
        super(generator);
    }

    /**
     * 获取下一个爬取时间间隔超过Config.interval的任务
     * 有下面几种情况可接受：
     * 1.爬取任务状态为UNFETCHED(未抓取)
     * 2.如果Config.interval为-1，表示时间间隔为无穷大，只能接受爬
     *   取任务状态为UNFETCHED(未抓取)的任务
     * 3.如果Config.interval>=0,且任务状态为已抓取，根据任务的抓取时间(fetchTime),加
     *   上时间间隔(Config.interval)，判断是否超过当前时间，如果超过，则接受任务。
     * @return 下一个达到时间间隔要求的爬取任务，如果没有符合规则的任务，返回null
     */
    @Override
    public CrawlDatum next() {
        while(true){
        CrawlDatum crawldatum=generator.next();
        
         if(crawldatum==null){
            return null;
        }
         
        
        if(crawldatum.getStatus()==CrawlDatum.STATUS_DB_UNFETCHED){
            return crawldatum;
        }
        if(Config.interval==-1){
            continue;
        }
       
        Long lasttime=crawldatum.getFetchTime();
        if(lasttime+Config.interval>System.currentTimeMillis()){
            continue;
        }
        return crawldatum;
        }
    }

   
    
}
