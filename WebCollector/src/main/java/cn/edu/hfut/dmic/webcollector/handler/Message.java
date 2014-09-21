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

package cn.edu.hfut.dmic.webcollector.handler;

/**
 * 消息
 * @author hu
 */
public class Message {

    /**
     * 消息附带的数据
     */
    public Object obj;

    /**
     * 消息的种类
     */
    public int what;
    
    /**
     * 构造一个空的消息
     */
    public Message(){
        
    }

    /**
     * 构造一个消息
     * @param what 消息的种类
     * @param obj 消息附带的数据
     */
    public Message(int what,Object obj) {
        this.what = what;
        this.obj = obj;
        
    }
    
    
    
    
    
    
}
