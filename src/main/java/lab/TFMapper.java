package lab;

import java.io.IOException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.*;
public class TFMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, TextArrayWritable> {


    int hashFunction(String text){
        long ret = 0;
        for(int j = 0 ; j < text.length() ; j++){
            ret *= Base;
            ret += text.charAt(j);
            ret %= MOD  ;
        }
        return (int) ret;
    }

    static int Base = 1023;
    static int MOD = 1500450271;

    private Text itemKey = new Text();
    private int ok = 1;
    @Override

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String data = value.toString();
        JsonObject obj = new JsonParser().parse(data).getAsJsonObject();
        String articleName = obj.get("title").toString();
        data = obj.get("text").toString() + ":";
        String [] field = data.toLowerCase().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");

        if(field == null) {
            return;
        }
        for(String token : field){
            if(token.isEmpty()) continue;
            if(!Character.isAlphabetic(token.charAt(0)) || !Character.isAlphabetic(token.charAt(0))) continue;
            itemKey.set(articleName);
            TextArrayWritable itemValue = new TextArrayWritable(new String[]{Integer.valueOf(hashFunction(token)).toString() , "1"});
            context.write(itemKey, itemValue);
        }

    }

}