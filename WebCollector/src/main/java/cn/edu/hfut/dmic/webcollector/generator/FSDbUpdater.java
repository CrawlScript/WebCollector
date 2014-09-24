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

import cn.edu.hfut.dmic.webcollector.fetcher.FSSegmentWriter;
import cn.edu.hfut.dmic.webcollector.fetcher.SegmentWriter;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Link;
import cn.edu.hfut.dmic.webcollector.parser.ParseData;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import cn.edu.hfut.dmic.webcollector.util.LogUtils;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;



/**
 *
 * @author hu
 */
public class FSDbUpdater implements DbUpdater{

    private SegmentWriter segmentWriter;
    private String crawlPath;
    private String segmentPath;
    private String segmentName;
    private DbWriter<CrawlDatum> updaterWriter;
    /**
     * 构建一个对指定爬取信息文件夹进行更新操作的更新器
     *
     * @param crawlPath
     */
    public FSDbUpdater(String crawlPath,String segmentName) {
        this.crawlPath = crawlPath;
        this.segmentName=segmentName;
        if(segmentName!=null){
            this.segmentPath=crawlPath + "/segments/" + segmentName;
        }
    }

    /**
     * 备份爬取任务列表
     * @throws IOException
     */
    public void backup() throws IOException {
        LogUtils.getLogger().info("backup "+getCrawlPath());
        File oldfile = new File(crawlPath, Config.old_info_path);
        File currentfile = new File(crawlPath, Config.current_info_path);
        FileUtils.copy(currentfile, oldfile);
    }

    /**
     * 判断更新器是否在上锁状态
     *
     * @return 是否上锁
     * @throws IOException
     */
    public boolean isLocked() throws IOException {
        File lockfile = new File(crawlPath + "/" + Config.lock_path);
        if (!lockfile.exists()) {
            return false;
        }
        String lock = new String(FileUtils.readFile(lockfile), "utf-8");
        return lock.equals("1");
    }

    /**
     * 上锁该更新器
     *
     * @throws IOException
     */
    public void lock() throws IOException {
        FileUtils.writeFile(crawlPath + "/" + Config.lock_path, "1".getBytes("utf-8"));
    }

    /**
     * 解锁该更新器
     *
     * @throws IOException
     */
    public void unlock() throws IOException {
        FileUtils.writeFile(crawlPath+ "/" + Config.lock_path, "0".getBytes("utf-8"));
    }
    // DataFileWriter<CrawlDatum> dataFileWriter;

    private void updateAll(ArrayList<CrawlDatum> datums) throws IOException {
        File currentfile = new File(crawlPath, Config.current_info_path);
        if (!currentfile.getParentFile().exists()) {
            currentfile.getParentFile().mkdirs();
        }
        DbWriter<CrawlDatum> writer = new DbWriter<CrawlDatum>(CrawlDatum.class, currentfile);
        for (CrawlDatum crawldatum : datums) {
            writer.write(crawldatum);
        }
        writer.close();
    }

    /**
     * 初始化该更新器
     *
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    public void initUpdater() throws UnsupportedEncodingException, IOException {
        File currentfile = new File(crawlPath, Config.current_info_path);
        if (!currentfile.getParentFile().exists()) {
            currentfile.getParentFile().mkdirs();
        }
        if (currentfile.exists()) {
            updaterWriter = new DbWriter<CrawlDatum>(CrawlDatum.class, currentfile, true);
        } else {
            updaterWriter = new DbWriter<CrawlDatum>(CrawlDatum.class, currentfile, false);
        }

    }

    /**
     * 关闭该更新器
     *
     * @throws IOException
     */
    public void closeUpdater() throws IOException {
        updaterWriter.close();
    }

    /**
     * 将爬取记录和爬取任务列表合并，更新爬取任务列表
     *
     * @param segment_path
     * @throws IOException
     */
    @Override
    public void merge() throws IOException {
        if(segmentName==null){
            return;
        }
        
        
        try {
            backup();     
        } catch (IOException ex) {
            LogUtils.getLogger().info("Exception",ex);
        }
        
        LogUtils.getLogger().info("merge " + segmentPath);
        
        File file_fetch = new File(segmentPath, "fetch/info.avro");
        if (!file_fetch.exists()) {
            return;
        }

        File file_current = new File(crawlPath, Config.current_info_path);
        DbReader<CrawlDatum> reader_current = new DbReader<CrawlDatum>(CrawlDatum.class, file_current);
        DbReader<CrawlDatum> reader_fetch = new DbReader<CrawlDatum>(CrawlDatum.class, file_fetch);

        HashMap<String, Integer> indexmap = new HashMap<String, Integer>();

        ArrayList<CrawlDatum> datums_origin = new ArrayList<CrawlDatum>();
        CrawlDatum datum = null;
        while (reader_current.hasNext()) {
            datum = reader_current.readNext();
            datums_origin.add(datum);
            indexmap.put(datum.getUrl(), datums_origin.size() - 1);
        }

        while (reader_fetch.hasNext()) {
            datum = reader_fetch.readNext();
            if (indexmap.containsKey(datum.getUrl())) {
                if (datum.getStatus() == CrawlDatum.STATUS_DB_UNFETCHED) {
                    continue;
                } else {
                    int preindex = indexmap.get(datum.getUrl());
                    datums_origin.set(preindex, datum);
                    indexmap.put(datum.getUrl(), preindex);
                }

            } else {
                datums_origin.add(datum);
                indexmap.put(datum.getUrl(), datums_origin.size() - 1);
            }

        }
        reader_fetch.close();

        File file_parse = new File(segmentPath, "parse_data/info.avro");
        if (file_parse.exists()) {
            DbReader<ParseData> reader_parse = new DbReader<ParseData>(ParseData.class, file_parse);
            ParseData parseresult = null;
            while (reader_parse.hasNext()) {
                parseresult = reader_parse.readNext();
                for (Link link : parseresult.getLinks()) {
                    datum = new CrawlDatum();
                    datum.setUrl(link.getUrl());
                    datum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
                    if (indexmap.containsKey(datum.getUrl())) {
                        continue;
                    } else {
                        datums_origin.add(datum);
                        indexmap.put(datum.getUrl(), datums_origin.size() - 1);
                    }
                }
            }
            reader_parse.close();
        }

        reader_current.close();

        updateAll(datums_origin);

    }

    

    @Override
    public SegmentWriter getSegmentWriter() {
        return segmentWriter;
    }

    @Override
    public void setSegmentWriter(SegmentWriter segmentWriter) {
        this.segmentWriter=segmentWriter;
    }

    public String getSegmentPath() {
        return segmentPath;
    }

    
   
    
    
  
    
    

    public static void main(String[] args) throws IOException {
        String crawl_path = "/home/hu/data/crawl_hfut1";
        //DbUpdater updater=new DbUpdater(crawl_path);
        //updater.merge2();
    }

    public String getCrawlPath() {
        return crawlPath;
    }

    public void setCrawlPath(String crawlPath) {
        this.crawlPath = crawlPath;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }


    
    
    
}
