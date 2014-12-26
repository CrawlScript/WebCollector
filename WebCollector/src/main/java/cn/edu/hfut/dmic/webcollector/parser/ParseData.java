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

import java.util.ArrayList;
import java.util.HashMap;
import org.apache.avro.reflect.Nullable;

import cn.edu.hfut.dmic.webcollector.model.Link;


/**
 *
 * @author hu
 */
public class ParseData {
    @Nullable 
    private String url;
   // @Nullable
   //private String title;
    @Nullable
    private ArrayList<Link> links;
    @Nullable
    private HashMap<String,String> parseMap=new HashMap<String, String>();
    
    public ParseData(){
        
    }
    
    public ParseData(String url,ArrayList<Link> links){
        this.url=url;
        this.links=links;
    }

    /*
    public ParseData(String url,String title,ArrayList<Link> links){
        this.url=url;
        this.title=title;
        this.links=links;
    }
    */

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    /*
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
    */

    public ArrayList<Link> getLinks() {
        return links;
    }

    public void setLinks(ArrayList<Link> links) {
        this.links = links;
    }

    public HashMap<String, String> getParseMap() {
        return parseMap;
    }

    public void setParseMap(HashMap<String, String> parseMap) {
        this.parseMap = parseMap;
    }
    
    
}
