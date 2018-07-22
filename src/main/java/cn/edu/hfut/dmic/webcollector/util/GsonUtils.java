package cn.edu.hfut.dmic.webcollector.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class GsonUtils {

    public static JsonParser jsonParser = new JsonParser();

    public static JsonElement parse(String json){
        return jsonParser.parse(json);
    }

}
