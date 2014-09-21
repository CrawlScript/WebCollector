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


import java.io.PrintWriter;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 *
 * @author hu
 */
public class LogUtils {
   
    private static Logger logger=null;
    static{
        logger=createCommonLogger("default");
    }
   
     public static Logger createCommonLogger(String defaultLogName) {
        Logger logger=Logger.getLogger(defaultLogName);
        ConsoleAppender ca = new ConsoleAppender();
        ca.setName("default");
        ca.setWriter(new PrintWriter(System.out));
        ca.setLayout(new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %p %c %x - %m%n "));
        logger.addAppender(ca);
        return logger;
    }

    public static Logger getLogger() {  
        return logger;
    }

    public static void setLogger(Logger logger) {
        LogUtils.logger = logger;
    }
    
   
    
    
   

    
}
