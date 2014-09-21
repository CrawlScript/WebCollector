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
 * 用于传递和处理消息类
 * @author hu
 */
public class Handler {
    
    /**
     * 发送一条消息
     * @param msg 待发送消息
     */
    public void sendMessage(Message msg){     
            handleMessage(msg);
    }
    
    /**
     * 处理消息，用户可以通过Override这个方法，来自定义处理消息的方法
     * @param msg
     */
    public void handleMessage(Message msg){
        
    }
    
}
