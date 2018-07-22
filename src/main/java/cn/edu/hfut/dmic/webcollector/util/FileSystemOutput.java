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

import cn.edu.hfut.dmic.webcollector.model.Page;
import java.io.File;
import java.net.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileSystemOutput并不属于WebCollector内核，它只是实现一个 简单的输出，将网页根据url路径，保存到本地目录，按照网站目录
 * 结构来存储网站内容。BreadthCrawler的visit函数中，默认使用
 * FileSystemOutput来保存网页。不推荐使用FileSystemOutput来 存储网页
 *
 * @author hu
 */
public class FileSystemOutput {

    public static final Logger LOG = LoggerFactory.getLogger(FileSystemOutput.class);

    protected String root;

    public FileSystemOutput(String root) {
        this.root = root;
    }

    public void output(Page page) {
        try {
            URL _URL = new URL(page.url());
            String query = "";
            if (_URL.getQuery() != null) {
                query = "_" + _URL.getQuery();
            }
            String path = _URL.getPath();
            if (path.length() == 0) {
                path = "index.html";
            } else {
                if (path.endsWith("/")) {
                    path = path + "index.html";
                } else {
                    int lastSlash = path.lastIndexOf("/");
                    int lastPoint = path.lastIndexOf(".");
                    if (lastPoint < lastSlash) {
                        path = path + ".html";
                    }
                }
            }
            path += query;
            File domain_path = new File(root, _URL.getHost());
            File f = new File(domain_path, path);
            FileUtils.write(f, page.content());
            LOG.info("output " + f.getAbsolutePath());
        } catch (Exception ex) {
            LOG.info("Exception", ex);
        }
    }
    

}
