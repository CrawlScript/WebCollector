/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.crawldb;

import cn.edu.hfut.dmic.webcollector.util.CrawlerConfiguration;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author hu
 */
public class SegmentUtil {

    public static void initSegments(Path crawlPath,Configuration conf) throws IOException {
        Path segmentsPath = new Path(crawlPath, "segments");
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(segmentsPath)) {
            fs.mkdirs(segmentsPath);
        }
    }

    public static String createSegment(Path crawlPath,Configuration conf) throws IOException {
        String segmentName = createSegmentName();
        FileSystem fs = FileSystem.get(conf);
        Path segmentPath = new Path(crawlPath, "segments/" + segmentName);
        fs.mkdirs(segmentPath);
        return segmentName;
    }

    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public synchronized static String createSegmentName() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
        }
        String segmentName = sdf.format(new Date());
        return segmentName;
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            System.out.println(createSegmentName());
        }
    }
}
