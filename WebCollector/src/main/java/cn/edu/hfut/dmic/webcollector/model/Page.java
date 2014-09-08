/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.model;

import cn.edu.hfut.dmic.webcollector.net.Response;
import cn.edu.hfut.dmic.webcollector.parser.ParseResult;

import org.jsoup.nodes.Document;

/**
 *
 * @author hu
 */
public class Page{
    private Response response=null;
    private String url=null;
  
   
    private String html=null;
    private Document doc=null;
    
    //public int status=;
    private long fetchTime;
    private ParseResult parseResult=null;
    
    public void setResponse(Response response){
        this.response=response;
    }
    
    public Response getResponse(){
        return response;
    }
    
    
    
    /*
    public static final int STATUS_UNDEFINED=-1;
    public static final int UNFETCHED=1;
    public static final int FETCHED=2;
    */
    
    //public static final int FETCHTIME_UNDEFINED=1;

    public byte[] getContent() {
        if(response==null)
            return null;
        return response.getContent();
    }

    

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getHtml() {
        return html;
    }

    public void setHtml(String html) {
        this.html = html;
    }

    public Document getDoc() {
        return doc;
    }

    public void setDoc(Document doc) {
        this.doc = doc;
    }

    public long getFetchTime() {
        return fetchTime;
    }

    public void setFetchTime(long fetchTime) {
        this.fetchTime = fetchTime;
    }

    

    public ParseResult getParseResult() {
        return parseResult;
    }

    public void setParseResult(ParseResult parseResult) {
        this.parseResult = parseResult;
    }
    
    
    
    
}
