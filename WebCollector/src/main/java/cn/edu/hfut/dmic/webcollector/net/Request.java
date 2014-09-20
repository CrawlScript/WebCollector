/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.net;


import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import java.net.Proxy;
import java.net.URL;

/**
 *
 * @author hu
 */
public interface Request {
    public URL getURL();
    public void setURL(URL url);
    
    //public void setProxy(Proxy proxy);
    //public Proxy getProxy();
    
    //public void setConnectionConfig(ConnectionConfig config);
    
    public Response getResponse(CrawlDatum datum) throws Exception;

}
