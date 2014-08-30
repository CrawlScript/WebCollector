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
    public String url;
    @Nullable
    public String title;
    @Nullable
    public ArrayList<Link> links;
    @Nullable
    public HashMap<String,String> parseMap=new HashMap<String, String>();
    
    public ParseData(){
        
    }
    public ParseData(String url,String title,ArrayList<Link> links){
        this.url=url;
        this.title=title;
        this.links=links;
    }
}
