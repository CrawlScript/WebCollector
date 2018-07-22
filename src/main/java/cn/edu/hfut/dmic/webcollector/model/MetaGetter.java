package cn.edu.hfut.dmic.webcollector.model;

import com.google.gson.JsonObject;

public interface MetaGetter {
    JsonObject meta();
    String meta(String key);
    int metaAsInt(String key);
    boolean metaAsBoolean(String key);
    double metaAsDouble(String key);
    long metaAsLong(String key);
    JsonObject copyMeta();
}
