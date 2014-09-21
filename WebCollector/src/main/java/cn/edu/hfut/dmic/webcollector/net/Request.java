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
