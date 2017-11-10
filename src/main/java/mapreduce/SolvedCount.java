package mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import common.Utils;

public class SolvedCount {

	public static class ProblemSolvedMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();
		private IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String json = value.toString();
			ObjectMapper mapper = new ObjectMapper();
			JsonNode node = mapper.readTree(json);
			ArrayNode result = (ArrayNode)node.get("result");
			
			Set<String> allSolvedProblems = new HashSet<String>();
			
			for (JsonNode sub : result) {
				if (Utils.filterProblem(sub)) {
					String problemName = sub.get("problem").get("name").getTextValue();
					allSolvedProblems.add(problemName);
				}
			}
			
			for (String problemName : allSolvedProblems) {
				word.set(problemName);
				System.out.println(problemName + " " + 1);
				context.write(word, one);
			}
		}
	}

	public static class IntegerSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
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
		//conf.set("mapreduce.job.running.map.limit","2");
		//conf.set("mapreduce.jobtracker.maxtasks.perjob","2");
		Job job = Job.getInstance(conf, "Submission count");
		job.setJarByClass(SolvedCount.class);
		job.setMapperClass(ProblemSolvedMapper.class);
		job.setCombinerClass(IntegerSumReducer.class);
		job.setReducerClass(IntegerSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		//TextInputFormat.setMaxInputSplitSize(job, 33554432);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}