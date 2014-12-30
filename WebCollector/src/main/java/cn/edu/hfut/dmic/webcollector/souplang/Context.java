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

package cn.edu.hfut.dmic.webcollector.souplang;


import java.util.ArrayList;
import java.util.HashMap;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author hu
 */
public class Context {
    public static final Logger LOG = LoggerFactory.getLogger(Context.class);
    public HashMap<String,Object> output=new HashMap<String, Object>();
    public Object get(String name){
        return output.get(name);
    }
    
    public String getString(String name){
        Object result=output.get(name);
        if(result==null){
            return null;
        }else{
            return result.toString();
        }
    }
    
    public ArrayList<String> getList(String name) throws Exception{
        Object value=output.get(name);
        
        if(value==null){
            return null;
        }else if(value instanceof ArrayList){
            ArrayList<String> result=(ArrayList<String>) value;
            return result;
        }else{
            throw new Exception("not a list");
        }
    }
}
