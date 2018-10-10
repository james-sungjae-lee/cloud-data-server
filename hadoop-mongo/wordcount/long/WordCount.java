import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import com.mongodb.hadoop.util.MongoConfigUtil;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.util.Iterator;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import org.bson.BSONObject;

import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends MongoTool {
	public WordCount() {
		this(new Configuration());
	}

	public WordCount(final Configuration conf) {
		setConf(conf);

		if (MongoTool.isMapRedV1()) {
			MapredMongoConfigUtil.setInputFormat(conf, com.mongodb.hadoop.mapred.MongoInputFormat.class);
			MapredMongoConfigUtil.setOutputFormat(conf, com.mongodb.hadoop.mapred.MongoOutputFormat.class);
		} else {
			MongoConfigUtil.setInputFormat(conf, MongoInputFormat.class);
			MongoConfigUtil.setOutputFormat(conf, MongoOutputFormat.class);
		}
		MongoConfigUtil.setMapper(conf, Map.class);
		MongoConfigUtil.setMapperOutputKey(conf, Text.class);
		MongoConfigUtil.setMapperOutputValue(conf, IntWritable.class);

		MongoConfigUtil.setReducer(conf, Reduce.class);
		MongoConfigUtil.setOutputKey(conf, Text.class);
		MongoConfigUtil.setOutputValue(conf, IntWritable.class);
	}

	public static void main(final String[] pArgs) throws Exception {
		System.exit(ToolRunner.run(new WordCount(), pArgs));
	}

	public static class Map extends Mapper<Object, BSONObject, Text, IntWritable>
			implements org.apache.hadoop.mapred.Mapper<Object, BSONWritable, Text, IntWritable> {
        private final IntWritable one = new IntWritable( 1 );
        private final Text word = new Text();

		@Override
		@SuppressWarnings("deprecation")
		public void map( Object key, BSONObject value, Context context ) throws IOException, InterruptedException{

            String[] genres = value.get("genres").toString().split("\\|");
            for(String genre : genres){
                word.set(genre);
                context.write(word, one);
            }
        }
		
		@Override
		public void map(Object arg0, BSONWritable arg1, OutputCollector<Text, IntWritable> arg2, Reporter arg3)
				throws IOException {

			BSONObject value = arg1.getDoc();
            String[] genres = value.get("genres").toString().split("\\|");
            for(String genre : genres){
                word.set(genre);
                arg2.collect(word, one);
            }
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public void configure(final JobConf job) {
		}

	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
			implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

		@Override
		public void reduce( Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException{

            int sum = 0;
            for ( final IntWritable val : values ){
                sum += val.get();
            }
            result.set( sum );
            context.write( key, result );
        }
		

		@Override
		public void reduce(final Text key, final Iterator<IntWritable> values, final OutputCollector<Text, IntWritable> output, final Reporter reporter) throws IOException {
            int sum = 0;
            while(values.hasNext()){
                sum += values.next().get();
            }
            result.set( sum );
            output.collect( key, result );
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public void configure(final JobConf job) {
		}
	}
}

