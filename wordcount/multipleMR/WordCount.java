package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount extends Configured implements Tool{

    public int run(String[] args) throws Exception {
      /*
       * Job 1
       */
      Configuration conf = getConf();
      Job job = Job.getInstance(conf, "Word Combined");
      job.setJarByClass(WordCount.class);

      job.setMapperClass(WordMapper.class);
      job.setReducerClass(SumReducer.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp"));

      job.waitForCompletion(true);

      /*
       * Job 2
       */
      Job job2 = Job.getInstance(conf, "Word Invert");

      job2.setNumReduceTasks(1);

      job2.setJarByClass(WordCount.class);
      job2.setMapperClass(WordMapper2.class);
      job2.setReducerClass(SumReducer2.class);

      job2.setOutputKeyClass(IntWritable.class);
      job2.setOutputValueClass(Text.class);

      job2.setInputFormatClass(KeyValueTextInputFormat.class);
      job2.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
      FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

      return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
      // TODO Auto-generated method stub
      if (args.length != 2) {
       System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
       System.exit(0);
      }
      ToolRunner.run(new Configuration(), new WordCount(), args);
     }

    public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineArray = line.split(",");
            String lastWord = lineArray[lineArray.length - 1];
            String[] genreArray = lastWord.split("\\|");
            for (String genre_value : genreArray) {
                word.set(genre_value);
                context.write(word, one);
            }
        }
    }

    public static class WordMapper2 extends Mapper<Text, Text, IntWritable, Text> {
        private Text word = new Text();
        IntWritable frequency = new IntWritable();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int newVal = Integer.parseInt(value.toString());
            frequency.set(newVal);
            context.write(frequency, key);
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class SumReducer2 extends Reducer<IntWritable, Text, Text, IntWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            for (Text val : values) {
            context.write(val, key);
            }
        }
    }
}
