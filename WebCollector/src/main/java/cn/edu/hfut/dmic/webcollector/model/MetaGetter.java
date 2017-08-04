package cn.edu.hfut.dmic.webcollector.model;

import com.google.gson.JsonObject;

public interface MetaGetter {

    public JsonObject meta();

    public String meta(String key);
    public int metaAsInt(String key);
    public boolean metaAsBoolean(String key);
    public double metaAsDouble(String key);
    public long metaAsLong(String key);
}
