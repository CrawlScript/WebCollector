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
package cn.edu.hfut.dmic.webcollector.extract;


import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.HttpRequest;
import cn.edu.hfut.dmic.webcollector.net.HttpResponse;
import java.lang.reflect.Constructor;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;


/**
 *
 * @author hu
 */
public class ExtractorTest {

    public static Extractor getInstance(Class<? extends Extractor> extractorClass, Page page) throws Exception {
        Constructor<? extends Extractor> cons=extractorClass.getDeclaredConstructor(Page.class);
        return cons.newInstance(page);

    }

    public static void testExtractorByUrl(String url, Class<? extends Extractor> extractorClass) throws Exception {
        HttpRequest httpRequest = new HttpRequest(url);
        HttpResponse response=httpRequest.getResponse();
        String htm=response.getHtmlByCharsetDetect();
        
        Page page=new Page();
        page.setUrl(url);
        page.setHtml(response.getHtmlByCharsetDetect());
        page.setResponse(response);
        
        Extractor extractor=getInstance(extractorClass, page);
        Links nextLinks=new Links();
        extractor.execute(nextLinks);
        for(String nextLink:nextLinks){
            System.out.println("nextLink:"+nextLink);
        }
    }
}
