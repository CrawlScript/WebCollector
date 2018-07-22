/*
 * Copyright (C) 2017 hu
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
package cn.edu.hfut.dmic.webcollector.conf;

import cn.edu.hfut.dmic.webcollector.util.Config;

import java.util.HashMap;
import java.util.Map;

/**
 * @author hu
 */
public class Configuration{

    public static final String KEY_MAX_EXECUTE_COUNT = "MAX_EXECUTE_COUNT";
    public static final String KEY_TOP_N = "TOP_N";

    public static final String KEY_CONNECT_TIMEOUT = "CONNECT_TIMEOUT";
    public static final String KEY_READ_TIMEOUT = "READ_TIMEOUT";



    public static final String KEY_EXECUTE_INTERVAL = "EXECUTE_INTERVAL";
    public static final String KEY_THREAD_KILLER = "THREAD_KILLER";
    public static final String KEY_WAIT_THREAD_END_TIME = "WAIT_THREAD_END_TIME";


    public static final String KEY_MAX_REDIRECT= "MAX_REDIRECT";
    public static final String KEY_MAX_RECEIVE_SIZE = "MAX_RECEIVE_SIZE";

    public static final String KEY_DEFAULT_USER_AGENT = "DEFAULT_USER_AGENT";
    public static final String KEY_DEFAULT_COOKIE = "COOKIE";

    public static final String KEY_AUTO_DETECT_IMG = "AUTO_DETECT_IMG";

    protected HashMap<String, Object> data = new HashMap<String, Object>();

    public Configuration set(String key, Object value){
        data.put(key, value);
        return this;
    }

    public String getString(String key){
        return get(key);
    }
    public Boolean getBoolean(String key){ return get(key); }
    public Integer getInteger(String key){
        return get(key);
    }
    public Long getLong(String key){
        return get(key);
    }
    public Double getDouble(String key){
        return get(key);
    }


    public <T> T get(String key){
        return (T)data.get(key);
    }

    public <T> T getOrDefault(String key, T defaultValue){
        Object value = data.get(key);
        if(value == null){
            return defaultValue;
        }else{
            return (T)value;
        }
    }

    public HashMap<String, Object> getData() {
        return data;
    }

    public void setData(HashMap<String, Object> data) {
        this.data = data;
    }


    protected Configuration copy(){
        Configuration conf = new Configuration();
        conf.data = (HashMap<String, Object>) data.clone();
        return conf;
    }


    public Integer getTopN(){
        return get(KEY_TOP_N);
    }
    public Configuration setTopN(Integer topN){
        return set(KEY_TOP_N, topN);
    }

    public Configuration setMaxExecuteCount(Integer maxExecuteCount){
        return set(KEY_MAX_EXECUTE_COUNT, maxExecuteCount);
    }
    public Integer getMaxExecuteCount(){
        return get(KEY_MAX_EXECUTE_COUNT);
    }

    public Configuration setConnectTimeout(Integer connectTimeOut){
        return set(KEY_CONNECT_TIMEOUT, connectTimeOut);
    }
    public Integer getConnectTimeout(){
        return get(KEY_CONNECT_TIMEOUT);
    }

    public Configuration setReadTimeout(Integer connectTimeOut){
        return set(KEY_READ_TIMEOUT, connectTimeOut);
    }
    public Integer getReadTimeout(){
        return get(KEY_READ_TIMEOUT);
    }

    public Integer getExecuteInterval(){
        return get(KEY_EXECUTE_INTERVAL);
    }
    public Configuration setExecuteInterval(Integer executeInterval){
        return set(KEY_EXECUTE_INTERVAL, executeInterval);
    }

    public Integer getThreadKiller(){
        return get(KEY_THREAD_KILLER);
    }

    public Configuration setThreadKiller(Integer threadKiller){
        return set(KEY_THREAD_KILLER, threadKiller);
    }

    public Integer getWaitThreadEndTime(){
        return get(KEY_WAIT_THREAD_END_TIME);
    }
    public Configuration setWaitThreadEndTime(Integer waitThreadEndTime){
        return set(KEY_WAIT_THREAD_END_TIME, waitThreadEndTime);
    }

    public Integer getMaxRedirect(){
        return get(KEY_MAX_REDIRECT);
    }
    public Configuration setMaxRedirect(Integer maxRedirect){
        return set(KEY_MAX_REDIRECT, maxRedirect);
    }

    public Integer getMaxReceiveSize(){
        return get(KEY_MAX_RECEIVE_SIZE);
    }
    public Configuration setMaxReceiveSize(int maxReceiveSize){
        return set(KEY_MAX_RECEIVE_SIZE, maxReceiveSize);
    }

    public String getDefaultUserAgent(){
        return get(KEY_DEFAULT_USER_AGENT);
    }
    public Configuration setDefaultUserAgent(String defaultUserAgent){
        return set(KEY_DEFAULT_USER_AGENT, defaultUserAgent);
    }

    public String getDefaultCookie(){
        return get(KEY_DEFAULT_COOKIE);
    }
    public Configuration setDefaultCookie(String defaultCookie){
        return set(KEY_DEFAULT_COOKIE, defaultCookie);
    }



    public Boolean getAutoDetectImg(){
        return get(KEY_AUTO_DETECT_IMG);
    }
    public Configuration setAutoDetectImg(Boolean autoDetectImg){
        return set(KEY_AUTO_DETECT_IMG, autoDetectImg);
    }


    private static Configuration defaultConf = null;

    private static Object configLock = new Object();

    public static Configuration getDefault(){
        if(defaultConf == null){
            synchronized (configLock){
                if(defaultConf == null){
                    defaultConf = new Configuration();
                    defaultConf.set(KEY_TOP_N, Config.TOP_N);
                    defaultConf.set(KEY_MAX_EXECUTE_COUNT, Config.MAX_EXECUTE_COUNT);
                    defaultConf.set(KEY_CONNECT_TIMEOUT, Config.TIMEOUT_CONNECT);
                    defaultConf.set(KEY_READ_TIMEOUT, Config.TIMEOUT_READ);
                    defaultConf.set(KEY_EXECUTE_INTERVAL, Config.EXECUTE_INTERVAL);
                    defaultConf.set(KEY_THREAD_KILLER, Config.THREAD_KILLER);
                    defaultConf.set(KEY_WAIT_THREAD_END_TIME, Config.WAIT_THREAD_END_TIME);
                    defaultConf.set(KEY_MAX_REDIRECT, Config.MAX_REDIRECT);
                    defaultConf.set(KEY_MAX_RECEIVE_SIZE, Config.MAX_RECEIVE_SIZE);
                    defaultConf.set(KEY_DEFAULT_USER_AGENT, Config.DEFAULT_USER_AGENT);
                    defaultConf.set(KEY_AUTO_DETECT_IMG, Config.AUTO_DETECT_IMG);
                }
            }
        }
        return defaultConf;
    }

    public static Configuration copyDefault(){
        return getDefault().copy();
    }


    @Override
    public String toString() {
        StringBuilder sb= new StringBuilder();
        sb.append("Configuration:\n");
        for(Map.Entry<String, Object> entry:data.entrySet()){
            sb.append("\t").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }
}
