package mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ThirdMiniPrj {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//MongoConfigUtil.setOutputURI(conf, "mongodb://127.0.0.1/prj.miniprj");
		Job job = Job.getInstance(conf, "wordcount");
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setJarByClass(ThirdMiniPrj.class);
		
		job.setMapperClass(Map.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
        
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//job.setOutputFormatClass(MongoOutputFormat.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class Map extends Mapper<Object, Text, Text, Text>{
		private Text k = new Text();
		private Text v = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			try {
				String jsonStr = value.toString();
				JSONParser jsonParser = new JSONParser();
				JSONObject jsonObj = (JSONObject) jsonParser.parse(jsonStr);
				JSONArray features = (JSONArray) jsonObj.get("features");
				JSONObject tempObj = null;
				String tempStr = "";
				
				for(int i=0 ; i<features.size() ; i++){
					String map = "{";
					tempObj = (JSONObject) features.get(i);
					k.set(tempObj.get("MNTN_CODE").toString());

					Iterator<String> is = tempObj.keySet().iterator();
					while(is.hasNext()) {
						tempStr = is.next();
						map += "\"" + tempStr + "\":\"" + tempObj.get(tempStr).toString() + "\",";
					}
					v.set(map.substring(0, map.length()-1) + "}");
					context.write(k, v);
				}
	        } catch (ParseException e) {
	            e.printStackTrace();
	        }
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String str = "";
			
			for (Text val : values) {
				str += val.toString();
			}
			
			result.set(str);
			context.write(key, result);
		}
	}

	
}