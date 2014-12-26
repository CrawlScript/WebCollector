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

package cn.edu.hfut.dmic.htmlbot;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;

/**
 * Created by a on 2014/11/2.
 */
public class HtmlBot {

    public static DomPage getDomPageByURL(String url) throws IOException {
        Document doc=Jsoup.connect(url).get();
        return new DomPage(doc);
    }

    public static DomPage getDomPageByHtml(String html){
        return getDomPageByHtml(html,null);
    }

    public static DomPage getDomPageByHtml(String html,String url){

        Document doc= Jsoup.parse(html);
        if(url!=null){
            doc.setBaseUri(url);
        }
        DomPage domPage=new DomPage(doc);
        return domPage;
    }
}
