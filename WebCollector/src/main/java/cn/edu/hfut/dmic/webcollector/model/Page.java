/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.model;

import java.util.List;
import java.util.Map;
import org.jsoup.nodes.Document;

/**
 *
 * @author hu
 */
public class Page{
    
    public String url=null;
    public byte[] content=null;
    public Map<String,List<String>> headers;
    public String html=null;
    public Document doc=null;
    public int status;
    public long fetchtime;
    
    public static final int STATUS_UNDEFINED=-1;
    public static final int UNFETCHED=1;
    public static final int FETCHED=2;
    
    public static final int FETCHTIME_UNDEFINED=1;
    
}
