package lab;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* This program can be used for
 * select count(1) from <table_name>;
 * Steps
 * 1. Develop mapper function
 * 2. Develop reducer function
 * 3. Job configuration
 */

public class Indexer extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Indexer(), args);
        System.exit(exitCode);

    }
    public int run(String[] args) throws Exception {
        Job tfJob = Job.getInstance(getConf(),
                "Row Count using built in mappers and reducers");

        tfJob.setJarByClass(getClass());
        Path outPath = new Path("/TFResults/");
        FileInputFormat.setInputPaths(tfJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(tfJob, new Path("/TFResults/"));

        FileSystem hdfs = FileSystem.get(getConf());
        if (hdfs.exists(outPath)) {
            hdfs.delete(outPath, true);
        }

        tfJob.setMapperClass(TFMapper.class);
        tfJob.setReducerClass(TFReducer.class);

        tfJob.setMapOutputKeyClass(Text.class);
        tfJob.setMapOutputValueClass(TextArrayWritable.class);

        tfJob.setOutputKeyClass(Text.class);
        tfJob.setOutputValueClass(Text.class);

        tfJob.waitForCompletion(true);





        Job idfJob = Job.getInstance(getConf(), "Row Count using built in mappers and reducers");

        idfJob.setJarByClass(getClass());

        outPath = new Path("/IDFResults/");
        if (hdfs.exists(outPath)) {
            hdfs.delete(outPath, true);
        }

        FileInputFormat.setInputPaths(idfJob, new Path("/TFResults/*"));
        FileOutputFormat.setOutputPath(idfJob, new Path("/IDFResults/"));

        idfJob.setMapperClass(IDFMapper.class);
        idfJob.setReducerClass(IDFReducer.class);

        idfJob.setMapOutputKeyClass(IntWritable.class);
        idfJob.setMapOutputValueClass(IntWritable.class);

        idfJob.setOutputKeyClass(IntWritable.class);
        idfJob.setOutputValueClass(IntWritable.class);

        return idfJob.waitForCompletion(true) ? 1 : 0;
    }

}