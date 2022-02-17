package mapReduce;

import java.io.IOException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ForestFire {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String jsonStr = value.toString();
	            JSONParser jsonParser = new JSONParser();
	            JSONObject jsonObj = (JSONObject) jsonParser.parse(jsonStr);
	            JSONArray items = (JSONArray) jsonObj.get("item");
	            JSONObject tempObj = null;
	            
	            for(Object o : items) {
	            	tempObj = (JSONObject)o;
	            	String because = tempObj.get("startyear") + "_" + tempObj.get("startmonth") + "_" + tempObj.get("firecause");
	            	
					word.set(because);
					context.write(word, one);
	            }
	            
	        } catch (ParseException e) {
	            e.printStackTrace();
	        }
		}
	}

	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
 
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//MongoConfigUtil.setOutputURI(conf, "mongodb://127.0.0.1/prj.miniprj");
		Job job = Job.getInstance(conf, "wordcount");
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setJarByClass(ForestFire.class);
		
		job.setMapperClass(TokenizerMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
        
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		MongoConfigUtil.setOutputURI(conf, args[1]);
		
		job.setOutputFormatClass(MongoOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}