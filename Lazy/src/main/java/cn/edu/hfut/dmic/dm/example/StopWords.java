package cn.edu.hfut.dmic.dm.example;

import java.io.*;
import java.util.HashMap;

/**
 * Created by hu on 15-12-22.
 */
public class StopWords {
    HashMap<String,Integer> words=new HashMap<String, Integer>();
    public StopWords() throws Exception {
        BufferedReader br=new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/stopwords.txt"),"utf-8"));
        String line;
        while((line=br.readLine())!=null){
            words.put(line,1);
        }
        br.close();
    }

    public boolean isStopWord(String word){
        return words.containsKey(word);
    }

    public static void main(String[] args) throws Exception {
        StopWords stopWords=new StopWords();
        System.out.println(stopWords.isStopWord("的"));
    }
}
