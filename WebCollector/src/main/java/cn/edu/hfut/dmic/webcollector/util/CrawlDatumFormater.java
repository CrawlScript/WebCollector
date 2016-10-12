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
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * @author hu
 */
public class CrawlDatumFormater {

    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String datumToString(CrawlDatum datum) {
        StringBuilder sb = new StringBuilder();
        sb.append("\nKEY: ").append(datum.getKey())
                .append("\nURL: ").append(datum.getUrl())
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

        sb.append("\nExecuteTime:").append(sdf.format(new Date(datum.getExecuteTime())))
                .append("\nExecuteCount:").append(datum.getExecuteCount());

        int metaIndex = 0;

        for (Entry<String, String> entry : datum.getMetaData().entrySet()) {
            sb.append("\nMETA").append("[").append(metaIndex++).append("]:(")
                    .append(entry.getKey()).append(",").append(entry.getValue()).append(")");
        }
        sb.append("\n");
        return sb.toString();
    }

    public static CrawlDatum jsonStrToDatum(String crawlDatumKey, String str) {
        JSONArray jsonArray = new JSONArray(str);
        CrawlDatum crawlDatum = new CrawlDatum();
        crawlDatum.setKey(crawlDatumKey);
        crawlDatum.setUrl(jsonArray.getString(0));
        crawlDatum.setStatus(jsonArray.getInt(1));
        crawlDatum.setExecuteTime(jsonArray.getLong(2));
        crawlDatum.setExecuteCount(jsonArray.getInt(3));
        if (jsonArray.length() == 5) {
            JSONObject metaJSONObject = jsonArray.getJSONObject(4);
            for (Object keyObject : metaJSONObject.keySet()) {
                String key = keyObject.toString();
                String value = metaJSONObject.getString(key);
                crawlDatum.meta(key, value);
            }
        }
        return crawlDatum;
    }

    public static String datumToJsonStr(CrawlDatum datum) {
        JSONArray jsonArray = new JSONArray();
        jsonArray.put(datum.getUrl());
        jsonArray.put(datum.getStatus());
        jsonArray.put(datum.getExecuteTime());
        jsonArray.put(datum.getExecuteCount());
        if (!datum.getMetaData().isEmpty()) {
            jsonArray.put(new JSONObject(datum.getMetaData()));
        }
        return jsonArray.toString();
    }

}
