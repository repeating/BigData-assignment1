package lab;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.lang.reflect.Array;
import java.util.HashMap;

public class TFReducer extends
        Reducer<Text, TextArrayWritable, Text, Text> {

    private Text result = new Text();


    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        HashMap < Integer , Integer > H = new HashMap<Integer, Integer>();
        for (TextArrayWritable arr : values) {
            Object tokens = arr.toArray();
            Text v1 =  (Text) Array.get(tokens , 0);
            Text v2 = (Text) Array.get(tokens , 1);

            int cur = 0;
            Integer a = Integer.parseInt(v1.toString());
            Integer ret = H.get(a);
            if(ret != null) cur = ret.intValue();
            ++cur;
            H.put(a , Integer.valueOf(cur));
        }
        String totret = new String("");
        int first = 1;
        for (Integer i : H.keySet()) {
            if(first == 1) first = 0;
            else totret = totret.concat(" ");
            Integer TF = (H.get(i));
            totret = totret.concat("(" + i.toString() + "," + TF.toString() + ")");

        }
        result.set(totret);
        context.write(new Text(key.toString() + ":") , result);

    }
}