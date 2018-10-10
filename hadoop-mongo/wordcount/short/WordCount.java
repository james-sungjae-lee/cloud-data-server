import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import org.bson.BSONObject;

public class WordCount {

    public static class Map extends Mapper<Object, BSONObject, Text, IntWritable> {

        private final static IntWritable one = new IntWritable( 1 );
        private final Text word = new Text();

        public void map( Object key, BSONObject value, Context context ) throws IOException, InterruptedException{

            String[] genres = value.get("genres").toString().split("\\|");
            for(String genre : genres){
                word.set(genre);
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        public void reduce( Text key, Iterable<IntWritable> values, Context context )
                throws IOException, InterruptedException{

            int sum = 0;
            for ( final IntWritable val : values ){
                sum += val.get();
            }
            result.set( sum );
            context.write( key, result );
        }
    }

    public static void main( String[] args ) throws Exception{

        final Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI( conf, "mongodb://"+args[0] );
        MongoConfigUtil.setOutputURI( conf, "mongodb://"+args[1] );

        System.out.println( "Conf: " + conf );

        final Job job = new Job( conf, "word count" );

        job.setJarByClass( WordCount.class );

        job.setMapperClass( Map.class );

        job.setCombinerClass( Reduce.class );
        job.setReducerClass( Reduce.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( IntWritable.class );

        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );

        System.exit( job.waitForCompletion( true ) ? 0 : 1 );
    }
}
