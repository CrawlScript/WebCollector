/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.net;


import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import java.net.URL;

/**
 * Http请求的接口，如果用户需要自定义实现Http请求的类，需要实现这个接口
 * @author hu
 */
public interface Request {
    public URL getURL();
    public void setURL(URL url); 
    
    public Response getResponse(CrawlDatum datum) throws Exception;

}
