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

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Http响应的接口，如果用户需要自定义http响应，需要实现这个接口
 * @author hu
 */
public interface Response {
   
  public URL getUrl();

 
  /**
   * 返回http响应码
   * @return 
   */
  public int getCode();


  /**
   * 返回指定http响应头字段的值。
   * @param name 头字段的名称
   * @return 
   */
  public List<String> getHeader(String name);
 
  /**
   * 返回http响应头字段的Map
   * @return http响应头字段的Map
   */
  public Map<String,List<String>> getHeaders();
  
  /**
   * 设置http响应头字段的Map
   * @param headers http响应头字段的Map
   */
  public void setHeaders(Map<String,List<String>> headers);
  
  /**
   * 返回http响应中的content-type，返回的content-type会影响到爬取/解析流程中
   * 对状态的判断
   * @return 
   */
  public String getContentType();
  
  
  /**
   * 返回网页/文件的内容(byte数组)
   * @return 网页/文件的内容(byte数组)
   */
  public byte[] getContent();
  
}
