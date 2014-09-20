/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
