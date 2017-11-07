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

import key.KeyGenre;
import mapreduce.CountryByGenreRating.*;

public class CountryByGenreRatingTest {

    MapDriver<Object, Text, KeyGenre, IntWritable> mapDriver;
    ReduceDriver<KeyGenre, IntWritable, KeyGenre, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, KeyGenre, IntWritable, KeyGenre, IntWritable> mapReduceDriver;
    
    @Before
    public void setUp() {
    	CountryGenreMapper mapper = new CountryGenreMapper();
    	mapper.setPathDir("src/test/resources/user.ratedList.trim.json");
        mapDriver = MapDriver.newMapDriver(mapper);
        IntegerTopKPctMeanReducer reducer = new IntegerTopKPctMeanReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
	
	@Test
	public void mapperTest() throws IOException {
		String text = "[uwip,implementation]	1000\n[uwip,brute force]	1200\n[uwi,brute force]	2000";
		
		mapDriver.withInput(new LongWritable(123), new Text(text));
		mapDriver.withOutput(new KeyGenre("Unknown","brute force"), new IntWritable(1200));
		mapDriver.withOutput(new KeyGenre("Japan","brute force"), new IntWritable(2000));
		mapDriver.withOutput(new KeyGenre("Unknown","implementation"), new IntWritable(1000));
		mapDriver.runTest(false);
	}
	
	@Test
	public void reducerTest() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1000));
		values.add(new IntWritable(1200));
		values.add(new IntWritable(2000));
		
		KeyGenre ug = new KeyGenre("Japan","implementation");
		reduceDriver.withInput(ug, values);
		reduceDriver.withOutput(ug, new IntWritable(2000));
		reduceDriver.runTest();
	}
	
	@Test
	public void mapReduceTest() throws IOException {
		String text = "[uwip,implementation]	1000\n[uwydoc,brute force]	1200\n[uwi,brute force]	2000";
		
		mapReduceDriver.withInput(new LongWritable(123), new Text(text));
		mapReduceDriver.withOutput(new KeyGenre("Japan","brute force"), new IntWritable(2000));
		mapReduceDriver.withOutput(new KeyGenre("Unknown","implementation"), new IntWritable(1000));
		mapReduceDriver.runTest(false);
	}
	
}
