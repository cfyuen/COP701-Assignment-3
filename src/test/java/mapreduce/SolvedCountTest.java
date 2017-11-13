package mapreduce;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import mapreduce.SolvedCount.ProblemSolvedMapper;
import mapreduce.SolvedCount.IntegerSumReducer;

public class SolvedCountTest {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    
    @Before
    public void setUp() {
    	ProblemSolvedMapper mapper = new ProblemSolvedMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        IntegerSumReducer reducer = new IntegerSumReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }
	
	@Test
	public void mapperTest() throws IOException {
		String json = "{\"status\":\"OK\",\"result\":[{\"id\":6746519,\"contestId\":435,\"creationTimeSeconds\":1401466328,\"relativeTimeSeconds\":2528,\"problem\":{\"contestId\":435,\"index\":\"C\",\"name\":\"Cardiogram\",\"type\":\"PROGRAMMING\",\"points\":1500.0,\"tags\":[\"implementation\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":46,\"timeConsumedMillis\":78,\"memoryConsumedBytes\":17817600},{\"id\":6743577,\"contestId\":435,\"creationTimeSeconds\":1401464730,\"relativeTimeSeconds\":930,\"problem\":{\"contestId\":435,\"index\":\"B\",\"name\":\"Pasha Maximizes\",\"type\":\"PROGRAMMING\",\"points\":1000.0,\"tags\":[\"greedy\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":46,\"timeConsumedMillis\":15,\"memoryConsumedBytes\":0},{\"id\":6742285,\"contestId\":435,\"creationTimeSeconds\":1401464255,\"relativeTimeSeconds\":455,\"problem\":{\"contestId\":435,\"index\":\"A\",\"name\":\"Queue on Bus Stop\",\"type\":\"PROGRAMMING\",\"points\":500.0,\"tags\":[\"implementation\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":34,\"timeConsumedMillis\":15,\"memoryConsumedBytes\":0},{\"id\":6741766,\"contestId\":435,\"creationTimeSeconds\":1401464122,\"relativeTimeSeconds\":322,\"problem\":{\"contestId\":435,\"index\":\"A\",\"name\":\"Queue on Bus Stop\",\"type\":\"PROGRAMMING\",\"points\":500.0,\"tags\":[\"implementation\"]},\"author\":{\"contestId\":435,\"members\":[{\"handle\":\"BenLaDeng\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":17,\"startTimeSeconds\":1401463800},\"programmingLanguage\":\"GNU C++\",\"verdict\":\"WRONG_ANSWER\",\"testset\":\"PRETESTS\",\"passedTestCount\":5,\"timeConsumedMillis\":15,\"memoryConsumedBytes\":0}]}";
		
		mapDriver.withInput(new LongWritable(123), new Text(json));
		mapDriver.withOutput(new Text("Cardiogram"), new IntWritable(1));
		mapDriver.withOutput(new Text("Pasha Maximizes"), new IntWritable(1));
		mapDriver.withOutput(new Text("Queue on Bus Stop"), new IntWritable(1));
		mapDriver.runTest(false);
	}
	
	@Test
	public void reducerTest() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		
		reduceDriver.withInput(new Text("Queue on Bus Stop"), values);
		reduceDriver.withOutput(new Text("Queue on Bus Stop"), new IntWritable(2));
		reduceDriver.runTest();
	}

}
