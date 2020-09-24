package lab;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.lang.reflect.Array;
import java.util.HashMap;

    public class IDFReducer extends
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private Text result = new Text();

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int all = 0;
        for(IntWritable t : values){
            all++;
        }
        context.write(key , new IntWritable(all));
    }
}