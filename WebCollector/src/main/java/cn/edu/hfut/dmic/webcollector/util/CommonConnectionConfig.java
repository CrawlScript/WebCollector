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

package cn.edu.hfut.dmic.webcollector.util;

import java.net.HttpURLConnection;

/**
 *
 * @author hu
 */
public class CommonConnectionConfig implements ConnectionConfig{

    public CommonConnectionConfig(String userAgent,String cookie) {
        this.userAgent=userAgent;
        this.cookie=cookie;
    }

    @Override
            public void config(HttpURLConnection con) {   
                if(userAgent!=null){
                    con.setRequestProperty("User-Agent", userAgent);
                }
                if (cookie != null) {
                    con.setRequestProperty("Cookie", cookie);
                }
            }
    

    
    private String userAgent=null;
    private String cookie=null;

   

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getCookie() {
        return cookie;
    }

    public void setCookie(String cookie) {
        this.cookie = cookie;
    }
    
    
    
    
}
