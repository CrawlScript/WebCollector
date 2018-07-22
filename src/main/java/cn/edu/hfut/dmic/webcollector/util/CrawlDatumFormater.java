/*
 * Copyright (C) 2015 hu
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

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * @author hu
 */
public class CrawlDatumFormater {

    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String datumToString(CrawlDatum datum) {
        StringBuilder sb = new StringBuilder();
        sb.append("\nKEY: ").append(datum.key())
                .append("\nURL: ").append(datum.url())
                .append("\nSTATUS: ");

        switch (datum.getStatus()) {
            case CrawlDatum.STATUS_DB_SUCCESS:
                sb.append("success");
                break;
            case CrawlDatum.STATUS_DB_FAILED:
                sb.append("failed");
                break;
            case CrawlDatum.STATUS_DB_UNEXECUTED:
                sb.append("unexecuted");
                break;
        }

        sb.append("\nExecuteTime: ")
                .append(sdf.format(new Date(datum.getExecuteTime())))
                .append("\nExecuteCount: ").append(datum.getExecuteCount())
                .append("\nCode: ").append(datum.code());

        String location = datum.location();
        if(location != null){
            sb.append("\nLocation: ").append(location);
        }

        int metaIndex = 0;

        for(Entry<String, JsonElement> entry: datum.meta().entrySet()){
            sb.append("\nMETA").append("[").append(metaIndex++).append("]: (")
                    .append(entry.getKey()).append(",").append(entry.getValue()).append(")");
        }


        sb.append("\n");
        return sb.toString();
    }

//    public static CrawlDatum jsonStrToDatum(String crawlDatumKey, String jsonStr) {
//        JsonArray jsonArray = GsonUtils.parse(jsonStr).getAsJsonArray();
//
//        CrawlDatum crawlDatum = new CrawlDatum();
//        crawlDatum.key(crawlDatumKey);
//        crawlDatum.url(jsonArray.get(0).getAsString());
//        crawlDatum.setStatus(jsonArray.get(1).getAsInt());
//        crawlDatum.setExecuteTime(jsonArray.get(2).getAsLong());
//        crawlDatum.setExecuteCount(jsonArray.get(3).getAsInt());
//        if (jsonArray.size() == 5) {
//            JsonObject metaJsonObject = jsonArray.get(4).getAsJsonObject();
//            crawlDatum.meta(metaJsonObject);
//        }
//        return crawlDatum;
//    }

//    public static String datumToJsonStr(CrawlDatum datum) {
//
//        JsonArray jsonArray = new JsonArray();
//        jsonArray.add(datum.url());
//        jsonArray.add(datum.getStatus());
//        jsonArray.add(datum.getExecuteTime());
//        jsonArray.add(datum.getExecuteCount());
//        if (datum.meta().size() > 0) {
//            jsonArray.add(datum.meta());
//        }
//
//        return jsonArray.toString();
//    }

}
