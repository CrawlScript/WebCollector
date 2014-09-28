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

package cn.edu.hfut.dmic.webcollector.generator;



import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;



/**
 * 广度遍历的种子注入器
 * @author hu
 */
public interface Injector{
    
    /**
     * 以新建的方式，注入一个种子url
     * @param url 种子url
     * @throws IOException
     */
    public void inject(String url) throws Exception;
    
    /**
     * 以新建的方式，注入种子url列表
     * @param urls 种子url列表
     * @throws IOException
     */
    public void inject(ArrayList<String> urls) throws Exception;
    
    /**
     * 以新建/追加的方式，注入一个种子url
     * @param url 种子url
     * @param append 是否追加
     * @throws IOException
     */
    public void inject(String url,boolean append) throws Exception;
    
        
    /**
     * 以新建/追加方式注入种子url列表
     * @param urls 种子url列表
     * @param append 是否追加
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    public void inject(ArrayList<String> urls,boolean append) throws Exception;
    
    /*
    public static void main(String[] args) throws IOException{
        Injector inject=new Injector("/home/hu/data/crawl_avro");
        inject.inject("http://www.xinhuanet.com/");
    }
    */
}
