package lab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.*;
import  java.io.*;
import java.io.IOException;
import java.util.HashMap;
import java.lang.System.*;

public class SearchEngine extends Configured implements Tool {

    //static int hashFunction(String text){

    //}




    private static class EngineMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, DoubleWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            int Base = 1023;
            int MOD = 1500450271;

            Configuration conf = context.getConfiguration();

            String query = conf.get("query");
            String[] tokens = query.split("\\s+");
            HashMap < Integer , Integer > H = new HashMap<Integer, Integer>();
            for (String token : tokens) {
                if(!token.isEmpty() && Character.isAlphabetic(token.charAt(0))) {
                    long ret = 0;
                    for(int j = 0 ; j < token.length() ; j++){
                        ret *= Base;
                        ret += token.charAt(j);
                        ret %= MOD  ;
                    }
                    int h = (int) ret;
                    int cur = 0;
                    Integer pick = H.get(Integer.valueOf(h));
                    if (pick != null) cur = pick.intValue();
                    ++cur;
                    H.put(h, cur);
                }
            }
            //String param = conf.get("test");

            //System.out.println(conf.get("query") + conf.get("lim"));

            String data = value.toString();
            String fname = data.substring(0 , data.indexOf(':'));
            data = data.substring(data.indexOf(':') , data.length());
            data = data.replaceAll("\\s+", " ");
            String[] field = data.split(" ", -1);
            if (field == null) {
                return;
            }



            //context.write(new DoubleWritable(-1.0 ), new Text(data));
            int cnt = 0;
            double result = 0.0;
            for (String iter : field) {
                String token = "";
                token = token.concat(iter).trim();
                if (token.charAt(0) != '(' || token.charAt(token.length() - 1) != ')') continue;
                token = token.replaceAll("[()]", "");
                token = token.trim();
                token = token.replaceAll(",", " ");
                String nums[] = token.split(" ", -1);
                int arg1 = -1, arg2 = -1;
                for (String word : nums) {
                    if (word.isEmpty()) continue;
                    if (Character.isDigit(word.charAt(0))) {
                        if (arg1 == -1) arg1 = Integer.parseInt(word);
                        else arg2 = Integer.parseInt(word);
                    }
                }
                //System.out.println(arg1 + " " + arg2);
                if (H.get(arg1) != null){
                 //   System.out.println("heyyyyyyyyyy");
                    result += arg2 * H.get(arg1).intValue();
                }
            }
            context.write(new DoubleWritable(-1.0 * result), new Text(fname));

        }
    }
    private static class EngineReducer extends Reducer<DoubleWritable, Text, IntWritable, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int Qlimit = Integer.parseInt(conf.get("lim"));
            for (Text res : values) {
                int curcnt = Integer.parseInt(conf.get("curcnt"));
                ++curcnt;
                conf.setInt("curcnt" , curcnt);
                context.write(new IntWritable(curcnt), res);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SearchEngine(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String query = args[1];
        query = query.toLowerCase().replaceAll("[^a-zA-Z ]", "").toLowerCase();
        conf.set("query" , query);
        conf.setInt("curcnt" , 0);
        conf.set("final" , "");
        int lim = (1<<30);
        if(args.length >= 3)
            lim = Integer.parseInt(args[2]);
        conf.setInt("lim" , lim);

        Job engineJob = Job.getInstance(conf
                ,"Row Count using built in mappers and reducers");
        engineJob.setJarByClass(getClass());

        String fPath = "/TFResults/*";
        Path fullPath = new Path(fPath);

        FileInputFormat.setInputPaths(engineJob, fullPath);
        FileOutputFormat.setOutputPath(engineJob, new Path(args[0]));

        FileSystem hdfs = FileSystem.get(getConf());
        if (hdfs.exists(new Path(args[0]))) {
            hdfs.delete(new Path(args[0]), true);
        }

        engineJob.setMapperClass(EngineMapper.class);
        engineJob.setReducerClass(EngineReducer.class);

        engineJob.setMapOutputKeyClass(DoubleWritable.class);
        engineJob.setMapOutputValueClass(Text.class);

        engineJob.setOutputKeyClass(IntWritable.class);
        engineJob.setOutputValueClass(Text.class);

        engineJob.waitForCompletion(true);

        //engineJob.
        int cnt = 0;

        conf = engineJob.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));
        Path[] paths = FileUtil.stat2Paths(fileStatus);
        for(Path path : paths) {
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line = "";;
            while ((line = br.readLine()) != null) {
                line = line.substring(line.indexOf("\""),line.length());
                System.out.println(++cnt + "-" +  line);
                if(cnt == lim) break;

            }

        }

        fs.close();
        return 1;

    }
}
