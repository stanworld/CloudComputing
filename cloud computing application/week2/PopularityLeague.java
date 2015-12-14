import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }
    
    
    @Override
    public int run(String[] args) throws Exception {
        // TODO
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Popularity League");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(PopularityCountMap.class);
        jobA.setReducerClass(PopularityCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(PopularityLeague.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Popularity League");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);

        jobB.setMapperClass(PopularityRankMap.class);
        jobB.setReducerClass(PopularityRankReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(PopularityLeague.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

     public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class PopularityCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        // TODO
       

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

    public static class PopularityCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        // TODO
      
        
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
            if(flag==1){
               context.write(key, new IntWritable(i-1));
            }
            else{
                context.write(key, new IntWritable(i));
            }
          }
        
    }

    public static class PopularityRankMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        Integer N;
        private TreeSet<Pair<Integer, Integer>> countTopLinkMap = new TreeSet<Pair<Integer, Integer>>();
        
        List<String> leaguelists;
        

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

            String stopWordsPath = conf.get("league");

            this.leaguelists = Arrays.asList(readHDFSFile(stopWordsPath, conf).split("\n"));
             this.N=16;
        }
      
        // TODO
         @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // TODO
          if (leaguelists.contains(key.toString().trim().toLowerCase())) {
            Integer count = Integer.parseInt(value.toString());
            Integer id = Integer.parseInt(key.toString());
            countTopLinkMap.add(new Pair<Integer, Integer>(count, id));
            if (countTopLinkMap.size() > N) {
                   countTopLinkMap.remove(countTopLinkMap.first());
            }
          }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // TODO
            for (Pair<Integer, Integer> item : countTopLinkMap) {
                Integer[] numbers = {item.second, item.first};
                IntArrayWritable val = new IntArrayWritable(numbers);
                context.write(NullWritable.get(), val);
            }
            
        }
        
    }

    public static class PopularityRankReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        Integer N;
        private TreeSet<Pair<Integer, Integer>> countTopLinkMap = new TreeSet<Pair<Integer, Integer>>();
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = 16;
        }
        // TODO
         public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            // TODO
            for (IntArrayWritable val: values) {
             IntWritable[] pair= (IntWritable[]) val.toArray();
             Integer id = pair[0].get();
             Integer count = pair[1].get();
             countTopLinkMap.add(new Pair<Integer,Integer>(count, id));
             if (countTopLinkMap.size() > N) {
 
                    countTopLinkMap.remove(countTopLinkMap.first());
             }
            }
            int i=0;
            int rank=0;
            Integer pre=0;
 //           for (Pair<Integer, Integer> item: countTopLinkMap) {
 //                 IntWritable id = new IntWritable(item.second);
 //                 IntWritable value = new IntWritable(item.first);
 //                 context.write(id, value);
 //           }
            for (Pair<Integer, Integer> item: countTopLinkMap) {
                if(i==0){
                  IntWritable id = new IntWritable(item.second);
                  
                  
                   pre=item.first;  
                  IntWritable value = new IntWritable(0);
                  context.write(id, value);
                  
                }
                else{
                  if(item.first> pre){
                    rank=i;
                    pre=item.first;
                  }
                  IntWritable id = new IntWritable(item.second);
                  IntWritable value = new IntWritable(rank);
                  context.write(id, value);
                  
                }
                i++;
            }
        }
    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}