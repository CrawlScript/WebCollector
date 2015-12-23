/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.crawldb;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.CrawlerConfiguration;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author hu
 */
public class DBReader {

    public static class DBReaderMapper extends Mapper<Text, CrawlDatum, Text, Text> {

        @Override
        protected void map(Text key, CrawlDatum value, Context context) throws IOException, InterruptedException {
            String datumStr = value.toString();
            System.out.println(datumStr);
            Text datumText = new Text(datumStr);
            context.write(new Text("::"), datumText);
        }

    }

    public static void main(String[] args) throws Exception {
        Path crawlPath = new Path("task2");
        Path currentPath = new Path(crawlPath, "crawldb/current");
        Path output = new Path("output");

        Configuration config = CrawlerConfiguration.create();
        FileSystem fs = FileSystem.get(config);

        if (fs.exists(output)) {
            fs.delete(output);
        }

        Job job = new Job(config);
        job.setJobName("dbreader "+crawlPath.toString());
        job.setMapperClass(DBReaderMapper.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, currentPath);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);

    }
}
