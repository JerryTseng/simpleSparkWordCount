package wordCount;

import java.util.Arrays;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// To test the git pull
		// To test the git push
		SparkConf conf = new SparkConf().setMaster("local").setAppName("My App"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load our input data.
		JavaRDD<String> input = sc.textFile("/home/jerry/new.txt");
		// Split up into words.
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() 
		{
			public Iterable<String> call(String x) 
			{ return Arrays.asList(x.split(" "));}
		});
		// Transform into pairs and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair( new PairFunction<String, String, Integer>()
		{
			public Tuple2<String, Integer> call(String x){ return new Tuple2(x, 1);}
		}).reduceByKey(new Function2<Integer, Integer, Integer>(){ 
			public Integer call(Integer x, Integer y){ return x + y;}
		});
		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile("/home/jerry/output");
	}
}
