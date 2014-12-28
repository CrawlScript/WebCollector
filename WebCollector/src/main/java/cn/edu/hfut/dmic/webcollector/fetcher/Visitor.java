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

package cn.edu.hfut.dmic.webcollector.fetcher;


import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import java.util.ArrayList;

/**
 *
 * @author hu
 */


/*
    Visitor是新API中，用户自定义的对每个页面访问的操作
*/
public  interface Visitor {
    
    
    /*访问页面page,并从页面中抽取页面中发现的需要爬取的URL,返回
      如果不需要从给定页面中发现新的链接，返回null*/
    public abstract Links visitAndGetNextLinks(Page page);
   

    
    
    
}
