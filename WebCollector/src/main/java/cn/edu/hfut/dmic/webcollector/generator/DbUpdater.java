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

import cn.edu.hfut.dmic.webcollector.fetcher.SegmentWriter;
import java.io.IOException;




/**
 * 用于更新爬取任务列表的类
 *
 * @author hu
 */
public interface DbUpdater {
    
    
  
    public void lock() throws Exception;
    public boolean isLocked() throws Exception;
    public void unlock() throws Exception;

    public void initSegmentWriter() throws Exception;
    public void close() throws Exception;
    public void merge() throws Exception;
    
    public SegmentWriter getSegmentWriter();
    //public void setSegmentWriter(SegmentWriter segmentWriter);
    public void clearHistory();
   
}
