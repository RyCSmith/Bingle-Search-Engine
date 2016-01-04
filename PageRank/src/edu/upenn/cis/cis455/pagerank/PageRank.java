package edu.upenn.cis.cis455.pagerank;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class PageRank {
	
	private final static double INITIAL_RANK = 1.0;
	
	@SuppressWarnings("serial")
	static class GraphGenerator implements PairFunction<String, String, List<String>> {


		public Tuple2<String, List<String>> call(String line) {
			String[] split = line.split(" ");
			
			/*
			if (split.length < 2) {
				throw new RuntimeException("poorly formatted file");
			}
			*/
			
			List<String> outEdges = new ArrayList<String>();
			for (int i = 1; i < split.length; i++) {
				outEdges.add(split[i]);
			}
			
			return new Tuple2<String, List<String>>(split[0], outEdges);
		}
	}
	
	static class RankGenerator implements Function<List<String>, Double> {

		@Override
		public Double call(List<String> outLinks) throws Exception {
			return INITIAL_RANK;
		}		
	}
	
	static class ContributionGenerator implements PairFlatMapFunction<Tuple2<List<String>, Double>, String, Double> {

		@Override
		public Iterable<Tuple2<String, Double>> call(Tuple2<List<String>, Double> page) throws Exception {
			Double contribution = page._2 / page._1.size();
			ArrayList<Tuple2<String, Double>> outgoingContributions = new ArrayList<Tuple2<String, Double>>();
			for (String outgoing : page._1) {
				outgoingContributions.add(new Tuple2<String, Double>(outgoing, contribution));
			}
			return outgoingContributions;
		}
		
	}
	
	static class RankTweaker implements Function<Double, Double> {
		
		private final double alpha = 1 / 7;

		@Override
		public Double call(Double rank) throws Exception {
			return rank * (1 - alpha) + alpha;
		}
		
	}
	
	static class Adder implements Function2<Double, Double, Double> {
		public Double call(Double x, Double y) {
			return x + y;
		}		
	}
	
	public static JavaPairRDD<String, Double> updateRank(
			JavaPairRDD<String, List<String>> pages, 
			JavaPairRDD<String, Double> ranks) {
		JavaRDD<Tuple2<List<String>, Double>> combined = pages.join(ranks).values();

		JavaPairRDD<String, Double> contributions = combined.flatMapToPair(new ContributionGenerator());
		JavaPairRDD<String, Double> updatedRanks  = contributions.reduceByKey(new Adder());
		updatedRanks.mapValues(new RankTweaker());
		
		return updatedRanks;
		
	}

	public static void main(String[] args) {
		String filename = "foo.txt";
		int iterations  = 100;
		
		SparkConf conf = new SparkConf().setAppName("Bingle PageRank");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = context.textFile(filename);
		
		JavaPairRDD<String, List<String>> vertices = lines.mapToPair(new GraphGenerator());
		JavaPairRDD<String, Double> ranks = vertices.mapValues(new RankGenerator());

		for (int i = 0; i < iterations; i++) {
			ranks = updateRank(vertices, ranks);
		}
		
		List<Tuple2<String, Double>> collected = ranks.collect();
		for (Tuple2<String, Double> tuple : collected) {
			System.out.print(tuple._1);
			System.out.print(" : ");
			System.out.println(tuple._2);
		}
		
		context.stop();
	}
	
}
