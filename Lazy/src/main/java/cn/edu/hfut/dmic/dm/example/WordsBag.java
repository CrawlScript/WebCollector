package cn.edu.hfut.dmic.dm.example;

import com.chenlb.mmseg4j.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

/**
 * Created by hu on 15-12-22.
 */
public class WordsBag {

    StopWords stopWords;
    Seg seg=new ComplexSeg(Dictionary.getInstance());
    public WordsBag() throws Exception {
        stopWords=new StopWords();
    }

    public HashMap<String,Integer> wordIndexMap=new HashMap<String, Integer>();

    public int currentInex=0;
    public int getWordIndex(String word){
        Integer index=wordIndexMap.get(word);
        if(index==null){
            wordIndexMap.put(word,currentInex++);
            return currentInex-1;
        }else{
            return index;
        }
    }


    public HashMap<Integer,Double> computeVectorMap(String text) throws IOException {
        HashMap<Integer,Double> result=new HashMap<Integer,Double>();
        MMSeg mmSeg=new MMSeg(new StringReader(text),seg);
        Word word=null;
        while((word=mmSeg.next())!=null){
            String wordStr=word.getString();
            if(!stopWords.isStopWord(wordStr)){
                int index=getWordIndex(wordStr);
                Double count=result.get(wordStr);
                if(count==null){
                    count=0.0;
                }
                count++;
                result.put(index,count);
            }

        }
        return result;
    }
}
