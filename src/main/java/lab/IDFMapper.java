package lab;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IDFMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, IntWritable, IntWritable> {

    private Text itemKey = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        data = data.replaceAll("\\s+"," ");
        String[] field = data.split(" ", -1);
        if(field == null) {
            return;
        }
        for(String iter : field){
            String token = ""; token = token.concat(iter).trim();
            if(token.charAt(0) != '(' || token.charAt(token.length() - 1) != ')') continue;
            token = token.replaceAll("[()]" , "");
            token = token.trim();
            token = token.replaceAll(","," ");
            String nums[] = token.split(" ",-1);
            for (String word : nums){
                if(word.isEmpty()) continue;
                if(Character.isDigit(word.charAt(0))) {
                    context.write(new IntWritable(Integer.parseInt(word)) ,new IntWritable(1));
                    break;
                }
            }
        }
    }

}