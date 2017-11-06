package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import mapreduce.MedianSolvedRating.*;

public class MedianSolvedRatingTest {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
    
    @Before
    public void setUp() {
    	ProblemSolvedRatingMapper mapper = new ProblemSolvedRatingMapper();
    	mapper.setPathDir("src/test/resources/user.ratedList.trim.json");
        mapDriver = MapDriver.newMapDriver(mapper);
        IntegerMedianReducer reducer = new IntegerMedianReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
	
	@Test
	public void mapperTest() throws IOException {
		String json = "{\"status\":\"OK\",\"result\":[{\"id\":20270331,\"contestId\":711,\"creationTimeSeconds\":1472534282,\"relativeTimeSeconds\":2147483647,\"problem\":{\"contestId\":711,\"index\":\"B\",\"name\":\"Chris and Magic Square\",\"type\":\"PROGRAMMING\",\"points\":1000.0,\"tags\":[\"constructive algorithms\"]},\"author\":{\"contestId\":711,\"members\":[{\"handle\":\"uwip\"}],\"participantType\":\"PRACTICE\",\"ghost\":false,\"startTimeSeconds\":1472472300},\"programmingLanguage\":\"GNU C++11\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":147,\"timeConsumedMillis\":686,\"memoryConsumedBytes\":2048000},{\"id\":20231589,\"contestId\":711,\"creationTimeSeconds\":1472472920,\"relativeTimeSeconds\":619,\"problem\":{\"contestId\":711,\"index\":\"A\",\"name\":\"Bus to Udayland\",\"type\":\"PROGRAMMING\",\"points\":500.0,\"tags\":[\"brute force\",\"implementation\"]},\"author\":{\"contestId\":711,\"members\":[{\"handle\":\"uwip\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":99,\"startTimeSeconds\":1472472300},\"programmingLanguage\":\"GNU C++11\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":71,\"timeConsumedMillis\":31,\"memoryConsumedBytes\":0},{\"id\":20002857,\"contestId\":707,\"creationTimeSeconds\":1471704720,\"relativeTimeSeconds\":6420,\"problem\":{\"contestId\":707,\"index\":\"A\",\"name\":\"Brain\u0027s Photos\",\"type\":\"PROGRAMMING\",\"points\":500.0,\"tags\":[\"implementation\"]},\"author\":{\"contestId\":707,\"members\":[{\"handle\":\"uwip\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":37,\"startTimeSeconds\":1471698300},\"programmingLanguage\":\"GNU C++11\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":50,\"timeConsumedMillis\":15,\"memoryConsumedBytes\":2048000}]}";
		
		mapDriver.withInput(new LongWritable(123), new Text(json));
		mapDriver.withOutput(new Text("Chris and Magic Square"), new IntWritable(1371));
		mapDriver.withOutput(new Text("Bus to Udayland"), new IntWritable(1371));
		mapDriver.withOutput(new Text("Brain\u0027s Photos"), new IntWritable(1371));
		mapDriver.runTest(false);
	}
	
	@Test
	public void reducerTest1() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1000));
		values.add(new IntWritable(2000));
		
		reduceDriver.withInput(new Text("Queue on Bus Stop"), values);
		reduceDriver.withOutput(new Text("Queue on Bus Stop"), new IntWritable(1500));
		reduceDriver.runTest();
	}
	
	@Test
	public void reducerTest2() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1000));
		values.add(new IntWritable(1800));
		values.add(new IntWritable(2000));
		
		reduceDriver.withInput(new Text("Queue on Bus Stop"), values);
		reduceDriver.withOutput(new Text("Queue on Bus Stop"), new IntWritable(1800));
		reduceDriver.runTest();
	}
	
	@Test
	public void mapReduceTest() throws IOException {
		String json1 = "{\"status\":\"OK\",\"result\":[{\"id\":20270331,\"contestId\":711,\"creationTimeSeconds\":1472534282,\"relativeTimeSeconds\":2147483647,\"problem\":{\"contestId\":711,\"index\":\"B\",\"name\":\"Chris and Magic Square\",\"type\":\"PROGRAMMING\",\"points\":1000.0,\"tags\":[\"constructive algorithms\"]},\"author\":{\"contestId\":711,\"members\":[{\"handle\":\"uwip\"}],\"participantType\":\"PRACTICE\",\"ghost\":false,\"startTimeSeconds\":1472472300},\"programmingLanguage\":\"GNU C++11\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":147,\"timeConsumedMillis\":686,\"memoryConsumedBytes\":2048000},{\"id\":20231589,\"contestId\":711,\"creationTimeSeconds\":1472472920,\"relativeTimeSeconds\":619,\"problem\":{\"contestId\":711,\"index\":\"A\",\"name\":\"Bus to Udayland\",\"type\":\"PROGRAMMING\",\"points\":500.0,\"tags\":[\"brute force\",\"implementation\"]},\"author\":{\"contestId\":711,\"members\":[{\"handle\":\"uwip\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":99,\"startTimeSeconds\":1472472300},\"programmingLanguage\":\"GNU C++11\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":71,\"timeConsumedMillis\":31,\"memoryConsumedBytes\":0},{\"id\":20002857,\"contestId\":707,\"creationTimeSeconds\":1471704720,\"relativeTimeSeconds\":6420,\"problem\":{\"contestId\":707,\"index\":\"A\",\"name\":\"Brain\u0027s Photos\",\"type\":\"PROGRAMMING\",\"points\":500.0,\"tags\":[\"implementation\"]},\"author\":{\"contestId\":707,\"members\":[{\"handle\":\"uwip\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":37,\"startTimeSeconds\":1471698300},\"programmingLanguage\":\"GNU C++11\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":50,\"timeConsumedMillis\":15,\"memoryConsumedBytes\":2048000}]}";
		String json2 = "{\"status\":\"OK\",\"result\":[{\"id\":20270331,\"contestId\":711,\"creationTimeSeconds\":1472534282,\"relativeTimeSeconds\":2147483647,\"problem\":{\"contestId\":711,\"index\":\"B\",\"name\":\"Chris and Magic Square\",\"type\":\"PROGRAMMING\",\"points\":1000.0,\"tags\":[\"constructive algorithms\"]},\"author\":{\"contestId\":711,\"members\":[{\"handle\":\"uwi\"}],\"participantType\":\"PRACTICE\",\"ghost\":false,\"startTimeSeconds\":1472472300},\"programmingLanguage\":\"GNU C++11\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":147,\"timeConsumedMillis\":686,\"memoryConsumedBytes\":2048000},{\"id\":20002857,\"contestId\":707,\"creationTimeSeconds\":1471704720,\"relativeTimeSeconds\":6420,\"problem\":{\"contestId\":707,\"index\":\"A\",\"name\":\"Brain\u0027s Photos\",\"type\":\"PROGRAMMING\",\"points\":500.0,\"tags\":[\"implementation\"]},\"author\":{\"contestId\":707,\"members\":[{\"handle\":\"uwi\"}],\"participantType\":\"CONTESTANT\",\"ghost\":false,\"room\":37,\"startTimeSeconds\":1471698300},\"programmingLanguage\":\"GNU C++11\",\"verdict\":\"OK\",\"testset\":\"TESTS\",\"passedTestCount\":50,\"timeConsumedMillis\":15,\"memoryConsumedBytes\":2048000}]}";
		
		mapReduceDriver.withInput(new LongWritable(123), new Text(json1));
		mapReduceDriver.withInput(new LongWritable(124), new Text(json2));
		mapReduceDriver.withOutput(new Text("Chris and Magic Square"), new IntWritable(2087));
		mapReduceDriver.withOutput(new Text("Bus to Udayland"), new IntWritable(1371));
		mapReduceDriver.withOutput(new Text("Brain\u0027s Photos"), new IntWritable(2087));
		mapReduceDriver.runTest(false);
	}

}
