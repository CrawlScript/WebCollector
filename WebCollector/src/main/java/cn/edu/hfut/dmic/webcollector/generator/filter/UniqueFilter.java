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
import java.util.HashSet;

/**
 * 唯一性过滤器
 * @author hu
 */
public class UniqueFilter extends Filter{
    
    /**
     * 构建一个唯一性过滤器
     * @param generator 嵌套的过滤器
     */
    public UniqueFilter(Generator generator) {
        super(generator);
    }

    private HashSet<String> hashset=new HashSet<String>();
    
    /**
     * 添加一个URL，以后该过滤器遇到相同URL会过滤
     * @param url
     */
    public void addUrl(String url){
         hashset.add(url);
    }

    /**
     * 获取下一个URL不重复的任务
     * @return 下一个URL不重复的爬取任务，如果没有符合规则的任务，返回null
     */

    @Override
    public CrawlDatum next() {
        while(true){
        CrawlDatum crawldatum=generator.next();
        if(crawldatum==null){
            return null;
        }
        String url=crawldatum.getUrl();
        if(hashset.contains(url)){
            continue;
        }
        else{
            addUrl(url);
            return crawldatum;
        }
        }
    }
    
}
