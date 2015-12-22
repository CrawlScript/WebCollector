package cn.edu.hfut.dmic.dm.example;

import cn.edu.hfut.dmic.dm.KMeans;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import com.chenlb.mmseg4j.*;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.jsoup.Jsoup;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 * Created by hu on 15-12-22.
 */
public class WebpageKmeans {


    public static void main(String[] args) throws Exception {


        MongoClient client = new MongoClient("127.0.0.1", 27017);
        MongoDatabase db = client.getDatabase("topics");
        MongoCollection col = db.getCollection("webpage");

        MongoCursor<Document> cursor = col.find().iterator();

        ArrayList<HashMap<Integer, Double>> vectorMapList = new ArrayList<HashMap<Integer, Double>>();
        ArrayList<String> results = new ArrayList<String>();

        WordsBag wordsBag = new WordsBag();

        while (cursor.hasNext()) {
            Document mongoDoc = cursor.next();
            String url = mongoDoc.getString("url");
            System.out.println(url);
            if (!Pattern.matches("https://ruby-china.org/topics/[0-9]+", url)) {
                continue;
            }
            String html = mongoDoc.getString("html");
            org.jsoup.nodes.Document jsoupDoc = Jsoup.parse(html);
            String title = jsoupDoc.title();
            String content = jsoupDoc.select("article").first().text().trim();
            HashMap<Integer, Double> vectorMap = wordsBag.computeVectorMap(content);
            vectorMapList.add(vectorMap);

            String result = title + "\t" + url;
            results.add(result);

        }
        int k = 8;
        KMeans kMeans = new KMeans(vectorMapList, wordsBag.currentInex, k);
        kMeans.start(100);

        File resultDir = new File("kmeans_result");
        if (resultDir.exists()) {
            FileUtils.deleteDir(resultDir);
        }

        resultDir.mkdir();


        BufferedWriter[] bws = new BufferedWriter[k];
        for (int i = 0; i < k; i++) {
            File resultFile = new File(resultDir, i + ".txt");
            bws[i] = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(resultFile), "utf-8"));
        }


        String line;
        for (int i = 0; i < results.size(); i++) {
            line = kMeans.labels[i] + "\t" + results.get(i) + "\n";
            bws[kMeans.labels[i]].write(line);
        }
        for (int i = 0; i < k; i++) {
            bws[i].close();
        }

        client.close();
    }
}
