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

package cn.edu.hfut.dmic.webcollector.parser;

import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.CharsetDetector;
import java.io.UnsupportedEncodingException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * 解析辅助类
 * @author hu
 */
public class ParseUtils {
    
    
    public static Document parseDocument(byte[] content,String url){
        String charset=CharsetDetector.guessEncoding(content);
        String html;
        try {
            html = new String(content,charset);
        } catch (UnsupportedEncodingException ex) {
            return null;
        }
        Document doc=Jsoup.parse(html);
        doc.setBaseUri(url);
        return doc;
    } 
    
    public static Page parseDocument(Page page){
        Document doc=parseDocument(page.getContent(), page.getUrl());
        page.setDoc(doc);
        return page;
    }
}
