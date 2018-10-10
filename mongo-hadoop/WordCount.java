import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

public class WordCount {

	public static class Map extends Mapper<Object, BSONObject, Text, Text> {

		private final Text titleText = new Text();
		private final Text genreText = new Text();

		public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {

			String title = value.get("title").toString();
			String genre = value.get("genres").toString().replace("\\|", ",");
			if (title.startsWith("a")) {
				titleText.set(title);
				genreText.set(genre);
				context.write(titleText, genreText);
			}
		}
	}

	public static class Combine extends Reducer<Text,Text,Text,Text>{
		private final Text CombinerText = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {

			CombinerText.set(values.iterator().next().toString());

			context.write(key, CombinerText);
		}
    }
	
	public static class Reduce extends Reducer<Text, Text, Text, BSONWritable> {
		private BSONWritable reduceResult = new BSONWritable();

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {

			BasicBSONObject output = new BasicBSONObject();
			output.put("genre", values.iterator().next().toString());
			reduceResult.setDoc(output);

			context.write(key, reduceResult);
		}
	}

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://" + args[0]);
		MongoConfigUtil.setOutputURI(conf, "mongodb://" + args[1]);
		System.out.println("Conf: " + conf);

		final Job job = new Job(conf, "word count");

		job.setJarByClass(WordCount.class);

		job.setMapperClass(Map.class);

		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BSONWritable.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
