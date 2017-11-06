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

import common.Utils;
import key.UserGenre;


/**
 * 
 * This will give out the average rating of top 10 difficult problems that each user solved by genre 
 * Map: Given user submission and problem rating, output [(user, genre), all solved problem rating] 
 * Reduce: Given [(user, genre), all solved problem rating], output [(user, genre), mean of top 10 rating]
 *
 */
public class UserByGenreRating {

	public static class UserGenreMapper extends Mapper<Object, Text, UserGenre, IntWritable> {

		private Map<String,Integer> problemRatingMap = new HashMap<String, Integer>();
		private Set<String> allSolvedProblems = new HashSet<String>();
		private Map<String,ArrayNode> problemGenre = new HashMap<String, ArrayNode>();
		
		private String pathDir = "problem.rating.txt";
		
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(pathDir);
			BufferedInputStream in = new BufferedInputStream(fs.open(path));
			String problemList = "";
			problemList = IOUtils.toString(in, "UTF-8");
		
			String[] problems = problemList.split("\n");
			for (String problem : problems) {
				String[] token = problem.split("\t");
				problemRatingMap.put(token[0],Integer.valueOf(token[1]));
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
					problemGenre.put(problemName, (ArrayNode) sub.get("problem").get("tags"));
					allSolvedProblems.add(problemName);
				}
			}
			
			for (String problemName : allSolvedProblems) {
				Integer rating = problemRatingMap.get(problemName);
				ArrayNode tags = problemGenre.get(problemName);
				for (JsonNode tag : tags) {
					UserGenre ug = new UserGenre(handle, tag.getTextValue());
					System.out.println(ug + " " + rating);
					context.write(ug, new IntWritable(rating));
				}
			}
		}

		public void setPathDir(String pathDir) {
			this.pathDir = pathDir;
		}
	}

	public static class IntegerTop10MeanReducer extends Reducer<UserGenre, IntWritable, UserGenre, IntWritable> {
		private List<Integer> ratingList = new ArrayList<Integer>();
		private List<Integer> topList;
		
		private final int TOP = 10;

		public void reduce(UserGenre key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			ratingList.clear();
			for (IntWritable val : values) {
				ratingList.add(val.get());
			}
			System.out.println(key + " " + ratingList);
			Collections.sort(ratingList);
			if (!ratingList.isEmpty()) {
				topList = ratingList.subList(Math.max(ratingList.size()-TOP, 0), ratingList.size());
				
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
		Job job = Job.getInstance(conf, "User genre top 10 rating");
		job.setJarByClass(UserByGenreRating.class);
		job.setMapperClass(UserGenreMapper.class);
		//job.setCombinerClass(IntegerTop10MeanReducer.class);
		job.setReducerClass(IntegerTop10MeanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}