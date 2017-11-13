package mapreduce;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import common.Utils;

public class ProblemGenre {

	public static class ProblemSolvedMapper extends Mapper<Object, Text, Text, NullWritable> {

		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String json = value.toString();
			ObjectMapper mapper = new ObjectMapper();
			JsonNode node = mapper.readTree(json);
			ArrayNode result = (ArrayNode)node.get("result");
			
			
			for (JsonNode sub : result) {
				if (Utils.filterProblem(sub)) {
					String problemName = sub.get("problem").get("name").getTextValue();
					ArrayNode genre = (ArrayNode)sub.get("problem").get("tags");
					String s=problemName+"\t";
					for(JsonNode tag : genre)
						s += tag.getTextValue() + ",";
					s = s.substring(0, s.length()-1);
					word.set(s);
					System.out.println(problemName + " null");
					context.write(word, NullWritable.get());
				}
			}
			
		}
	}

	public static class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
		
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.set("mapreduce.job.running.map.limit","2");
		//conf.set("mapreduce.jobtracker.maxtasks.perjob","2");
		Job job = Job.getInstance(conf, "Distinct Problems");
		job.setJarByClass(ProblemGenre.class);
		job.setMapperClass(ProblemSolvedMapper.class);
		job.setCombinerClass(DistinctReducer.class);
		job.setReducerClass(DistinctReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		//TextInputFormat.setMaxInputSplitSize(job, 33554432);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}