package mapreduce;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import common.UserInfo;
import key.KeyGenre;


/**
 * 
 * This will give out the average rating of top 10 difficult problems that each user solved by genre 
 * Map: Given [(user, genre), all solved problem rating], output [(country, genre), rating] 
 * Reduce: Given [(country, genre), rating], output [(country, genre), mean of top K% rating]
 *
 */
public class CountryByGenreRating {

	public static class CountryGenreMapper extends Mapper<Object, Text, KeyGenre, IntWritable> {

		private Map<String,UserInfo> userMap = new HashMap<String, UserInfo>();
		
		private String pathDir = "user.ratedList.json";
		
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			
			FileSystem fs = FileSystem.get(conf);
			
			Path path = new Path(pathDir);
			BufferedInputStream in = new BufferedInputStream(fs.open(path));
			String userList = "";
			userList = IOUtils.toString(in, "UTF-8");
		
			ObjectMapper mapper = new ObjectMapper();
			JsonNode node = mapper.readTree(userList);
			ArrayNode result = (ArrayNode)node.get("result");
			for (JsonNode user : result) {
				String handle = user.get("handle").getTextValue();
				UserInfo userInfo = new UserInfo();
				if (user.get("country") == null) {
					userInfo.setCountry("Unknown");
				}
				else {
					userInfo.setCountry(user.get("country").getTextValue());
				}
				userInfo.setRating(user.get("rating").getIntValue());
				userMap.put(handle, userInfo);
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String file = value.toString();
			String[] lines = file.split("\n");
			
			for (String line : lines) {
				String[] tokens = line.split("\t");
				KeyGenre kg = new KeyGenre(tokens[0]);
				Integer rating = Integer.valueOf(tokens[1]);
				
				String country = userMap.get(kg.getKey()).getCountry();
				kg.setKey(country);
				
				System.out.println(kg + " " + rating);
				context.write(kg, new IntWritable(rating));
			}
			
		}

		public void setPathDir(String pathDir) {
			this.pathDir = pathDir;
		}
	}

	public static class IntegerTopKPctMeanReducer extends Reducer<KeyGenre, IntWritable, KeyGenre, IntWritable> {
		private List<Integer> ratingList = new ArrayList<Integer>();
		private List<Integer> topList;
		
		private int topKPct = 10;

		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			
			topKPct = conf.getInt("topk.pct", 10);
		}
		
		public void reduce(KeyGenre key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			ratingList.clear();
			for (IntWritable val : values) {
				ratingList.add(val.get());
			}
			System.out.println(key + " " + ratingList);
			Collections.sort(ratingList);
			if (!ratingList.isEmpty()) {
				int topK = (int)(Math.ceil(ratingList.size()*topKPct/100.0));
				System.out.println(key + " Top K % is " + topK + " out of " + ratingList.size());
				topList = ratingList.subList(Math.max(ratingList.size() - topK, 0), ratingList.size());
				
				Integer sum = 0;
				for (Integer top : topList) {
					sum += top;
				}
				Integer avg = sum / topList.size();
				context.write(key, new IntWritable(avg));
				
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Country genre top K pct rating");
		job.setJarByClass(CountryByGenreRating.class);
		job.setMapperClass(CountryGenreMapper.class);
		//job.setCombinerClass(IntegerTop10MeanReducer.class);
		job.setReducerClass(IntegerTopKPctMeanReducer.class);
		job.setOutputKeyClass(KeyGenre.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}