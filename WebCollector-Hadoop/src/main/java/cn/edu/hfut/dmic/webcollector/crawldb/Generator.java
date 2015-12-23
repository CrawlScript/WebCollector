/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.crawldb;


import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 *
 * @author hu
 */
public class Generator {

 

    public static class GeneratorReducer extends Reducer<Text, CrawlDatum, Text, CrawlDatum> {

        int limit;
        int count = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println("-------------------------reducer:"+context.getNumReduceTasks());
            int topN = context.getConfiguration().getInt("generator.topN", Integer.MAX_VALUE);
            
            limit =  topN / context.getNumReduceTasks();
            
            System.out.println("=================limit="+limit);
            
        }

        @Override
        protected void reduce(Text key, Iterable<CrawlDatum> values, Context context) throws IOException, InterruptedException {
            Iterator<CrawlDatum> ite = values.iterator();
            while (ite.hasNext() && count < limit) {
                CrawlDatum value = ite.next();
                if (value.getStatus() == CrawlDatum.STATUS_DB_UNFETCHED && value.getRetry() <= 20) {
                    context.write(key, value);
                    count++;
                    context.getCounter("generator", "count").increment(1);
                }
            }

        }

    }

    public static String generate(Path crawlPath,Configuration conf) throws Exception {
        SegmentUtil.initSegments(crawlPath,conf);
        String segmentName = SegmentUtil.createSegment(crawlPath,conf);

        Path currentPath = new Path(crawlPath, "crawldb/current");
        Path generatePath = new Path(crawlPath, "segments/" + segmentName + "/generate");

        
      

        Job job = new Job(conf);
        job.setJobName("generate "+crawlPath.toString());
        job.setJarByClass(Generator.class);
        
        job.setReducerClass(GeneratorReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CrawlDatum.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CrawlDatum.class);
        FileInputFormat.addInputPath(job, currentPath);
        FileOutputFormat.setOutputPath(job, generatePath);
        job.waitForCompletion(true);
        long count=job.getCounters().findCounter("generator", "count").getValue();
        System.out.println("total generate:"+count);
        if(count==0){
            return null;
        }else{
            return segmentName;
        }

    }

   
}
