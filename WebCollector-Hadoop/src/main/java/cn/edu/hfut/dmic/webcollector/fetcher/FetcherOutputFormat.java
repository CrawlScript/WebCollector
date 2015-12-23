/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.fetcher;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Content;
import cn.edu.hfut.dmic.webcollector.model.ParseData;
import cn.edu.hfut.dmic.webcollector.model.Redirect;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 *
 * @author hu
 */
public class FetcherOutputFormat extends OutputFormat<Text, Writable> {
    
    @Override
    public RecordWriter<Text, Writable> getRecordWriter(TaskAttemptContext tac) throws IOException, InterruptedException {
        Configuration conf = tac.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        String outputPath = conf.get("mapred.output.dir");
        
        Path fetchPath = new Path(outputPath, "fetch/info");
        Path contentPath = new Path(outputPath, "content/info");
        Path parseDataPath = new Path(outputPath, "parse/info");
        Path redirectPath = new Path(outputPath, "redirect/info");
        final SequenceFile.Writer fetchOut = new SequenceFile.Writer(fs, conf, fetchPath, Text.class, CrawlDatum.class);
        final SequenceFile.Writer contentOut = new SequenceFile.Writer(fs, conf, contentPath, Text.class, Content.class);
        final SequenceFile.Writer parseDataOut = new SequenceFile.Writer(fs, conf, parseDataPath, Text.class, CrawlDatum.class);
        final SequenceFile.Writer redirectOut = new SequenceFile.Writer(fs, conf, redirectPath,CrawlDatum.class,Text.class);
       
        return new RecordWriter<Text, Writable>() {
            
            @Override
            public void write(Text k, Writable v) throws IOException, InterruptedException {
                if (v instanceof CrawlDatum) {
                    fetchOut.append(k, v);
                } else if (v instanceof Content) {
                    contentOut.append(k, v);
                } else if (v instanceof ParseData) {
                    
                    ParseData parseData = (ParseData) v;
                    CrawlDatums next = parseData.next;
                    for (CrawlDatum datum : next) {
                        parseDataOut.append(new Text(datum.getKey()), datum);
                    }
                    
                }else if(v instanceof Redirect){
                    Redirect redirect=(Redirect) v;
                    redirectOut.append(redirect.datum,new Text(redirect.realUrl));
                }
            }
            
            @Override
            public void close(TaskAttemptContext tac) throws IOException, InterruptedException {
                fetchOut.close();
                contentOut.close();
                parseDataOut.close();
                redirectOut.close();
            }
        };
        
    }
    
    @Override
    public void checkOutputSpecs(JobContext jc) throws IOException, InterruptedException {
    }
    
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext tac) throws IOException, InterruptedException {
        return new FileOutputCommitter(null, tac);
    }
    
}
