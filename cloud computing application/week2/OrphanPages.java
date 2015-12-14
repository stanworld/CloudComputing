/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author stan
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        //TODO        
        Job job = Job.getInstance(this.getConf(), "Orphan Pages");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(OrphanPageReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(OrphanPages.class);
        return job.waitForCompletion(true) ? 0 : 1;
        
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //TODO
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, " :");
            int i=0;
            int ss=0;
            
            int st;
           while (tokenizer.hasMoreTokens()) {
               String nextToken = tokenizer.nextToken();
               if(i==0){
                 ss=Integer.parseInt(nextToken);
                 i=1;
                 context.write(new IntWritable(ss), new IntWritable(-1));
               }
               else {
                   if(nextToken != null && !nextToken.isEmpty()){
                      st=Integer.parseInt(nextToken);
                      context.write(new IntWritable(st), new IntWritable(ss));
                   }
                }  
           }
            
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
            int i = 0;
            int flag=0;
            for (IntWritable val : values) {
                  i=i+1;
                  if(val.get()==-1){
                      flag=1;
                  }
            }
            if(i==1){
				           if(flag==1){
                        context.write(key, NullWritable.get());
				           }
            }
           
        }
    }
}
