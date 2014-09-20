/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.parser;

import cn.edu.hfut.dmic.webcollector.model.Link;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.avro.reflect.Nullable;

/**
 *
 * @author hu
 */
public class ParseData {
    @Nullable 
    private String url;
    @Nullable
    private String title;
    @Nullable
    private ArrayList<Link> links;
    @Nullable
    private HashMap<String,String> parseMap=new HashMap<String, String>();
    
    public ParseData(){
        
    }
    public ParseData(String url,String title,ArrayList<Link> links){
        this.url=url;
        this.title=title;
        this.links=links;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

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
