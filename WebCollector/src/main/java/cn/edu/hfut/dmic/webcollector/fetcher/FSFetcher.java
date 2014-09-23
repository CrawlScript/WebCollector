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

import cn.edu.hfut.dmic.webcollector.fetcher.FSSegmentWriter;
import cn.edu.hfut.dmic.webcollector.fetcher.Fetcher;
import cn.edu.hfut.dmic.webcollector.fetcher.SegmentWriter;
import cn.edu.hfut.dmic.webcollector.generator.DbUpdater;
import cn.edu.hfut.dmic.webcollector.generator.FSDbUpdater;
import cn.edu.hfut.dmic.webcollector.util.LogUtils;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.log4j.Priority;

/**
 *
 * @author hu
 */
public class FSFetcher extends Fetcher{
    private String crawlPath;
    
    protected String getLastSegmentName(){
        String[] segment_list=new File(crawlPath,"segments").list();
        if(segment_list==null){
            return null;
        }
        String segment_path=null;
        long max=0;
        for(String segment:segment_list){
            long timestamp=Long.valueOf(segment);
            if(timestamp>max){
                max=timestamp;
                segment_path=segment;
            }
        }
        return segment_path;
    }
    
    public FSFetcher() {
        super();
    } 
    
     /**
     * 构建一个Fetcher,抓取器会将爬取信息存储在crawlPath目录中
     * @param crawlPath
     */
    public FSFetcher(String crawlPath) {
        this.crawlPath = crawlPath;
        setNeedUpdateDb(true);
    }
    
    @Override
    protected DbUpdater createDbUpdater(){
        FSDbUpdater fsDbUpdater=new FSDbUpdater(crawlPath, getSegmentName());
        fsDbUpdater.setSegmentWriter(new FSSegmentWriter(crawlPath,getSegmentName()));
        try {
            fsDbUpdater.backup();
        } catch (IOException ex) {
            LogUtils.getLogger().info("Exception",ex);
        }
        return fsDbUpdater;
    }

    @Override
    protected DbUpdater createRecoverDbUpdater() {
        DbUpdater recoverDbUpdater=new FSDbUpdater(crawlPath, getLastSegmentName());
        
        return recoverDbUpdater;
    }
     
     
}
