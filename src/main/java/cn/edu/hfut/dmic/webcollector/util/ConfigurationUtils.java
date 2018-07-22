package cn.edu.hfut.dmic.webcollector.util;

import cn.edu.hfut.dmic.webcollector.conf.Configuration;
import cn.edu.hfut.dmic.webcollector.conf.Configured;

public class ConfigurationUtils {
//    public static void addParent(Object child, Configured parent){
//        if(child instanceof Configured){
//            Configured configuredChild = (Configured) child;
//            configuredChild.setParent(parent);
//        }
//    }

    public static void setTo(Configured from, Object... targets){
        for(Object target:targets){
            if(target instanceof Configured){
                Configured configuredTarget = (Configured) target;
                configuredTarget.setConf(from.getConf());
            }
        }

    }

}
