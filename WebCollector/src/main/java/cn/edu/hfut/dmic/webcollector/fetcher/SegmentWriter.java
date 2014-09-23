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


import cn.edu.hfut.dmic.webcollector.model.Content;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.parser.ParseResult;



/**
 * 爬取过程中，写入爬取历史、网页Content、解析信息的Writer
 * @author hu
 */
public interface SegmentWriter {

   /**
     * 写入一条爬取历史记录
     * @param fetch 爬取历史记录（爬取任务)
     * @throws IOException
     */
    public  void wrtieFetch(CrawlDatum fetch) throws Exception;

    /**
     * 写入一条Content对象(存储网页/文件内容的对象)
     * @param content
     * @throws IOException
     */
    public void wrtieContent(Content content) throws Exception;

    /**
     * 写入一条网页解析结果
     * @param parseresult 网页解析结果
     * @throws IOException
     */
    public void wrtieParse(ParseResult parseresult) throws Exception;

    /**
     * 关闭Writer
     * @throws IOException
     */
    public void close() throws Exception;
}
