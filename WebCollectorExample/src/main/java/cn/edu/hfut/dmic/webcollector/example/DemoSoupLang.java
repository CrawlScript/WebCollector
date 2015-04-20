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

package cn.edu.hfut.dmic.webcollector.example;

import cn.edu.hfut.dmic.webcollector.example.souplang.Context;
import cn.edu.hfut.dmic.webcollector.example.souplang.SoupLang;
import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.xml.sax.SAXException;

/**
 *
 * @author hu
 */
public class DemoSoupLang {
    public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException{
        Document doc=Jsoup.connect("http://www.youdao.com/search?q=webcollector")
                .userAgent("Mozilla/5.0 (X11; Linux i686; rv:34.0) Gecko/20100101 Firefox/34.0")
                .get();
        
        SoupLang soupLang=new SoupLang(ClassLoader.getSystemResourceAsStream("example/DemoRule2.xml"));
        Context context=soupLang.extract(doc);        
        System.out.println(context);
    }
}
