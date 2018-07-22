package cn.edu.hfut.dmic.webcollector.model;

import com.google.gson.JsonObject;

public interface MetaSetter <T>{
    T meta(JsonObject metaData);
    T meta(String key, String value);
    T meta(String key, int value);
    T meta(String key, boolean value);
    T meta(String key, double value);
    T meta(String key, long value);
}
