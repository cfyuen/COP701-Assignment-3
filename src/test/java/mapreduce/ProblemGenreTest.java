package mapreduce;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import mapreduce.ProblemGenre.ProblemSolvedMapper;
import mapreduce.ProblemGenre.DistinctReducer;

public class ProblemGenreTest {

    MapDriver<Object, Text, Text, NullWritable> mapDriver;
    ReduceDriver<Text, NullWritable, Text, NullWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, NullWritable, Text, NullWritable> mapReduceDriver;
    
    @Before
    public void setUp() {
    	ProblemSolvedMapper mapper = new ProblemSolvedMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        DistinctReducer reducer = new DistinctReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
    }
	
	@Test
	public void mapperTest() throws IOException {
		String json = "{\"status\":\"OK\",\"result\":[{\"id\":6746519,\"contestId\":435,\"creationTimeSeconds\":1401466328,\"relativeTimeSeconds\":2528,\"problem\":{\"contestId\":435,\"index\":\"C\",\"name\":\"Cardiogram\",\"type\":\"PROGRAMMING\",\"points\":1500.0,\"tags\":[\"implementation\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":46,\"timeConsumedMillis\":78,\"memoryConsumedBytes\":17817600},{\"id\":6743577,\"contestId\":435,\"creationTimeSeconds\":1401464730,\"relativeTimeSeconds\":930,\"problem\":{\"contestId\":435,\"index\":\"B\",\"name\":\"Pasha Maximizes\",\"type\":\"PROGRAMMING\",\"points\":1000.0,\"tags\":[\"greedy\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":46,\"timeConsumedMillis\":15,\"memoryConsumedBytes\":0},{\"id\":6742285,\"contestId\":435,\"creationTimeSeconds\":1401464255,\"relativeTimeSeconds\":455,\"problem\":{\"contestId\":435,\"index\":\"A\",\"name\":\"Queue on Bus Stop\",\"type\":\"PROGRAMMING\",\"points\":500.0,\"tags\":[\"implementation\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":34,\"timeConsumedMillis\":15,\"memoryConsumedBytes\":0},{\"id\":6741766,\"contestId\":435,\"creationTimeSeconds\":1401464122,\"relativeTimeSeconds\":322,\"problem\":{\"contestId\":435,\"index\":\"A\",\"name\":\"Queue on Bus Stop\",\"type\":\"PROGRAMMING\",\"points\":500.0,\"tags\":[\"implementation\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"WRONG_ANSWER\",\"testset\":\"PRETESTS\",\"passedTestCount\":5,\"timeConsumedMillis\":15,\"memoryConsumedBytes\":0}]}";
		
		mapDriver.withInput(new LongWritable(123), new Text(json));
		mapDriver.withOutput(new Text("Cardiogram\timplementation"), NullWritable.get());
		mapDriver.withOutput(new Text("Pasha Maximizes\tgreedy"), NullWritable.get());
		mapDriver.withOutput(new Text("Queue on Bus Stop\timplementation"), NullWritable.get());
		mapDriver.runTest(false);
	}
	
	@Test
	public void reducerTest() throws IOException {
		List<NullWritable> values = new ArrayList<NullWritable>();
		values.add(NullWritable.get());
		values.add(NullWritable.get());
		values.add(NullWritable.get());
		
		reduceDriver.withInput(new Text("Queue on Bus Stop\tgreedy,dp"), values);
		reduceDriver.withOutput(new Text("Queue on Bus Stop\tgreedy,dp"), NullWritable.get());
		reduceDriver.runTest();
	}
	
	@Test
	public void mapReduceTest() throws IOException {
		String json1 = "{\"status\":\"OK\",\"result\":[{\"id\":6746519,\"contestId\":435,\"creationTimeSeconds\":1401466328,\"relativeTimeSeconds\":2528,\"problem\":{\"contestId\":435,\"index\":\"C\",\"name\":\"Cardiogram\",\"type\":\"PROGRAMMING\",\"points\":1500.0,\"tags\":[\"implementation,dp,greedy\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":46,\"timeConsumedMillis\":78,\"memoryConsumedBytes\":17817600},{\"id\":6743577,\"contestId\":435,\"creationTimeSeconds\":1401464730,\"relativeTimeSeconds\":930,\"problem\":{\"contestId\":435,\"index\":\"B\",\"name\":\"Pasha Maximizes\",\"type\":\"PROGRAMMING\",\"points\":1000.0,\"tags\":[\"greedy\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":46,\"timeConsumedMillis\":15,\"memoryConsumedBytes\":0},{\"id\":6746519,\"contestId\":435,\"creationTimeSeconds\":1401466328,\"relativeTimeSeconds\":2528,\"problem\":{\"contestId\":435,\"index\":\"C\",\"name\":\"Cardiogram\",\"type\":\"PROGRAMMING\",\"points\":1500.0,\"tags\":[\"implementation,dp,greedy\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":46,\"timeConsumedMillis\":78,\"memoryConsumedBytes\":17817600},{\"id\":6742285,\"contestId\":435,\"creationTimeSeconds\":1401464255,\"relativeTimeSeconds\":455,\"problem\":{\"contestId\":435,\"index\":\"A\",\"name\":\"Queue on Bus Stop\",\"type\":\"PROGRAMMING\",\"points\":500.0,\"tags\":[\"implementation\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":34,\"timeConsumedMillis\":15,\"memoryConsumedBytes\":0},{\"id\":6741766,\"contestId\":435,\"creationTimeSeconds\":1401464122,\"relativeTimeSeconds\":322,\"problem\":{\"contestId\":435,\"index\":\"A\",\"name\":\"Queue on Bus Stop\",\"type\":\"PROGRAMMING\",\"points\":500.0,\"tags\":[\"implementation\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"WRONG_ANSWER\",\"testset\":\"PRETESTS\",\"passedTestCount\":5,\"timeConsumedMillis\":15,\"memoryConsumedBytes\":0},{\"id\":6746519,\"contestId\":435,\"creationTimeSeconds\":1401466328,\"relativeTimeSeconds\":2528,\"problem\":{\"contestId\":435,\"index\":\"C\",\"name\":\"Cardiogram\",\"type\":\"PROGRAMMING\",\"points\":1500.0,\"tags\":[\"implementation,dp,greedy\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":46,\"timeConsumedMillis\":78,\"memoryConsumedBytes\":17817600}]}";
		mapReduceDriver.withInput(new LongWritable(123), new Text(json1));
		mapReduceDriver.withOutput(new Text("Cardiogram\timplementation,dp,greedy"), NullWritable.get());
		mapReduceDriver.withOutput(new Text("Pasha Maximizes\tgreedy"), NullWritable.get());
		mapReduceDriver.withOutput(new Text("Queue on Bus Stop\timplementation"), NullWritable.get());
		mapReduceDriver.runTest(false);
	}
}
