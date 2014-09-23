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

import cn.edu.hfut.dmic.webcollector.generator.DbWriter;
import cn.edu.hfut.dmic.webcollector.model.Content;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.parser.ParseData;
import cn.edu.hfut.dmic.webcollector.parser.ParseResult;
import cn.edu.hfut.dmic.webcollector.parser.ParseText;
import cn.edu.hfut.dmic.webcollector.util.Config;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hu
 */
public class FSSegmentWriter implements SegmentWriter{
    

    private String segmentPath;

    /**
     * 构造一个在指定文件夹写爬取信息的Writer
     * @param segmentPath 指定的文件夹路径
     */
    public FSSegmentWriter(String crawlPath,String segmentName) {
        this.segmentPath =  crawlPath + "/segments/" + segmentName;
        count_fetch = 0;
        count_content = 0;
        count_parse = 0;

        try {
            fetchWriter = new DbWriter<CrawlDatum>(CrawlDatum.class, segmentPath + "/fetch/info.avro");
            contentWriter = new DbWriter<Content>(Content.class, segmentPath + "/content/info.avro");
            parseDataWriter = new DbWriter<ParseData>(ParseData.class, segmentPath + "/parse_data/info.avro");
            parseTextWriter = new DbWriter<ParseText>(ParseText.class, segmentPath + "/parse_text/info.avro");
        } catch (IOException ex) {
            Logger.getLogger(SegmentWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private DbWriter<CrawlDatum> fetchWriter;
    private DbWriter<Content> contentWriter;
    private DbWriter<ParseData> parseDataWriter;
    private DbWriter<ParseText> parseTextWriter;
    private int count_content;
    private int count_parse;
    private int count_fetch;

    /**
     * 写入一条爬取历史记录
     * @param fetch 爬取历史记录（爬取任务)
     * @throws IOException
     */
    public synchronized void wrtieFetch(CrawlDatum fetch) throws IOException {
        fetchWriter.write(fetch);
        count_fetch = (count_fetch++) % Config.segmentwriter_buffer_size;
        if (count_fetch == 0) {
            fetchWriter.flush();
        }
    }

    /**
     * 写入一条Content对象(存储网页/文件内容的对象)
     * @param content
     * @throws IOException
     */
    public synchronized void wrtieContent(Content content) throws IOException {
        contentWriter.write(content);
        count_content = (count_content++) % Config.segmentwriter_buffer_size;
        if (count_content == 0) {
            contentWriter.flush();
        }
    }

    /**
     * 写入一条网页解析结果
     * @param parseresult 网页解析结果
     * @throws IOException
     */
    public synchronized void wrtieParse(ParseResult parseresult) throws IOException {
        parseDataWriter.write(parseresult.getParsedata());
        parseTextWriter.write(parseresult.getParsetext());
        count_parse = (count_parse++) % Config.segmentwriter_buffer_size;
        if (count_parse == 0) {
            parseDataWriter.flush();
            parseTextWriter.flush();
        }
    }

    /**
     * 关闭Writer
     * @throws IOException
     */
    public void close() throws IOException {
        fetchWriter.close();
        contentWriter.close();
        parseDataWriter.close();
        parseTextWriter.close();
    }
}
