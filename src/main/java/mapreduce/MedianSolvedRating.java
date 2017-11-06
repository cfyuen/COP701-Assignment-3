package mapreduce;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import common.Utils;

/**
 * 
 * This will give out the average difficulty of a problem
 * Map: Given user submission and user rating, output [problem, user rating who solved it]
 * Reduce: Given [problem, user rating who solved it], output [problem, median of user rating who solved it]
 *
 */
public class MedianSolvedRating {

	public static class ProblemSolvedRatingMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();
		
		private Map<String,UserInfo> userMap = new HashMap<String, UserInfo>();
		private Set<String> allSolvedProblems = new HashSet<String>();
		
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
			String json = value.toString();
			ObjectMapper mapper = new ObjectMapper();
			JsonNode node = mapper.readTree(json);
			ArrayNode result = (ArrayNode)node.get("result");
			
			allSolvedProblems.clear();
			
			String handle = "";
			
			for (JsonNode sub : result) {
				if (Utils.filterProblem(sub)) {
					handle = sub.get("author").get("members").get(0).get("handle").getTextValue();
					String problemName = sub.get("problem").get("name").getTextValue();
					allSolvedProblems.add(problemName);
				}
			}
			
			for (String problemName : allSolvedProblems) {
				word.set(problemName);
				Integer rating = userMap.get(handle).getRating();
				System.out.println(problemName + " " + rating);
				context.write(word, new IntWritable(rating));
			}
		}

		public void setPathDir(String pathDir) {
			this.pathDir = pathDir;
		}
	}

	public static class IntegerMedianReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private List<Integer> ratingList = new ArrayList<Integer>();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			ratingList.clear();
			for (IntWritable val : values) {
				ratingList.add(val.get());
			}
			System.out.println(key + " " + ratingList);
			Collections.sort(ratingList);
			if (!ratingList.isEmpty()) {
				if (ratingList.size() % 2 == 0) {
					Integer m1 = ratingList.get(ratingList.size() / 2 - 1);
					Integer m2 = ratingList.get(ratingList.size() / 2);
					context.write(key, new IntWritable((m1+m2)/2));
				}
				else {
					Integer m = ratingList.get(ratingList.size() / 2);
					context.write(key, new IntWritable(m));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Submission median rating");
		job.setJarByClass(MedianSolvedRating.class);
		job.setMapperClass(ProblemSolvedRatingMapper.class);
		//job.setCombinerClass(IntegerMedianReducer.class);
		job.setReducerClass(IntegerMedianReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}