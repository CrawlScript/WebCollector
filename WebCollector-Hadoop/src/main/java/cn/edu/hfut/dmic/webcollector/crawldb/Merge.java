/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.crawldb;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.CrawlerConfiguration;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 *
 * @author hu
 */
public class Merge{

    public static void merge(Path crawlPath, Path[] mergePaths,Configuration conf,String jobName) throws Exception {
        
        Job job = new Job(conf);
        job.setJobName(jobName+"  "+crawlPath.toString());
        job.setJarByClass(Merge.class);
        // job.getConfiguration().set("mapred", "/home/hu/mygit/WebCollector2/WebCollectorCluster/target/WebCollectorCluster-2.0.jar");
        Path crawldbPath = new Path(crawlPath, "crawldb");
        Path newdb = new Path(crawldbPath, "new");
        Path currentdb = new Path(crawldbPath, "current");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(currentdb)) {
            FileInputFormat.addInputPath(job, currentdb);
        }

        if (fs.exists(newdb)) {
            fs.delete(newdb);
        }
        for (Path mergePath : mergePaths) {
            FileInputFormat.addInputPath(job, mergePath);
        }
        FileOutputFormat.setOutputPath(job, newdb);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CrawlDatum.class);

        job.setMapperClass(MergeMap.class);
        job.setReducerClass(MergeReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CrawlDatum.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.waitForCompletion(true);

    }

   

  

    public static void install(Path crawlPath,Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path crawldbPath = new Path(crawlPath, "crawldb");
        Path newdb = new Path(crawldbPath, "new");
        Path currentdb = new Path(crawldbPath, "current");
        Path olddb = new Path(crawldbPath, "old");
        if (fs.exists(currentdb)) {
            if (fs.exists(olddb)) {
                fs.delete(olddb);
            }
            fs.rename(currentdb, olddb);
        }
        fs.rename(newdb, currentdb);
    }

    public static class MergeMap extends Mapper<Text, CrawlDatum, Text, CrawlDatum> {

        @Override
        protected void map(Text key, CrawlDatum value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key), value);
        }

    }

    public static class MergeReduce extends Reducer<Text, CrawlDatum, Text, CrawlDatum> {

        @Override
        protected void reduce(Text key, Iterable<CrawlDatum> values, Context context) throws IOException, InterruptedException {
            Iterator<CrawlDatum> ite = values.iterator();
            CrawlDatum temp = null;
            while (ite.hasNext()) {

                CrawlDatum nextDatum = ite.next().copy();
                if (nextDatum.getStatus() == CrawlDatum.STATUS_DB_FORCED_INJECT) {
                    nextDatum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
                    temp = nextDatum;
                    break;
                }

                 if (nextDatum.getStatus() == CrawlDatum.STATUS_DB_INJECT) {
                    nextDatum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
                }
                 
                if (temp == null) {
                    temp = nextDatum;
                    continue;
                }
               

                if (nextDatum.getStatus() > temp.getStatus()) {
                    temp = nextDatum;
                    continue;
                }
            }
            if (temp != null) {
                context.write(key, temp);
            }

        }

    }
}
