package fr.uha.bigdata;

import static fr.uha.bigdata.utils.Utils.*;

import fr.uha.bigdata.utils.CloseableIterator;
import scala.Tuple2;
import scala.Tuple4;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
	 
	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("demo");

		String master = System.getProperty("spark.master");
		if (master == null || master.trim().length() == 0) {
			System.out.println("No master found ; running locally");
			conf.setMaster("local[*]")
				.set("spark.driver.host", "127.0.0.1")
				;
		} else {
			System.out.println("Master found to be " + master);
		}

		// Tries to determine necessary jar
		String source = Main.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (source.endsWith(".jar")) {
			conf.setJars(new String [] {source});
		}
		File infile = new File("data.csv");
		downloadIfAbsent(new URL("https://svn.ensisa.uha.fr/bd/co2/owid-co2-data-continents.csv"), infile);

		try (JavaSparkContext sc = new JavaSparkContext(conf)) {

			JavaRDD<String[]> raw = sc.textFile(infile.getAbsolutePath(), 32)
				.map(l -> fromCSV(l, ','))
				.filter(a -> a.length > 0)
				;

			final String[] headers = raw.first();
			final Map<String, Integer> h = new TreeMap<>();
			for(int i = 0; i < headers.length; ++i) {
				h.put(headers[i], i);
			}

			raw = raw.filter(a -> !Arrays.equals(a, headers));

			JavaRDD<Tuple4<String /* continent */, String /* country */, String /* GHG */, String /* GDP */>> _2016 = raw
				.filter(a -> "2016".equals(a[h.get("year")]))
				.map(a -> new Tuple4<>(a[h.get("continent")], a[h.get("country")], a[h.get("total_ghg")], a[h.get("gdp")]))
				.filter(t -> !t._1().isEmpty() && ! t._2().isEmpty() && !t._3().isEmpty() && !t._4().isEmpty())
				;
			
			JavaPairRDD<Tuple2<String /* country */,String>,Short /* emission factor */> emissions = _2016.mapToPair(c -> {
				float ghg = Float.parseFloat(c._3());

				String gdpStr = c._4();
				int idx = gdpStr.indexOf('.');
				if (idx >= 0) gdpStr = gdpStr.substring(0, idx);
				long gdp = Long.parseLong(gdpStr);

				short e = (short)(gdp / (long)(ghg * 1_000_000));
				return new Tuple2<>(new Tuple2<>(c._2(), c._1()), e);
			});


			JavaRDD<Tuple4<String, String, String, String>> _2006 = raw.filter(a->"2006".equals(a[h.get("year")]))
			.map(a -> new Tuple4<>(a[h.get("continent")], a[h.get("country")], a[h.get("total_ghg")], a[h.get("gdp")]))
			.filter(t -> !t._1().isEmpty() && ! t._2().isEmpty() && !t._3().isEmpty() && !t._4().isEmpty())
			;

			JavaPairRDD<Tuple2<String,String>, Short> emissions2006 = _2006.mapToPair(c -> {
				float ghg = Float.parseFloat(c._3());

				String gdpStr = c._4();
				int idx = gdpStr.indexOf('.');
				if (idx >= 0) gdpStr = gdpStr.substring(0, idx);
				long gdp = Long.parseLong(gdpStr);

				short e = (short)(gdp / (long)(ghg * 1_000_000));
				return new Tuple2<>(new Tuple2<>(c._2(), c._1()), e);
			});

			
			//List<Tuple2<String, Tuple2<String, Short>>> res = emissions.take(20);
			//res.forEach(System.out::println);
			//res.forEach(a -> System.out.println(Arrays.toString(a)));

			/*JavaPairRDD<String, Tuple2<String, Short>> best = emissions
				.reduceByKey((c1, c2) -> c1._2() > c2._2() ? c1 : c2)
				;
			
			List<Tuple2<String, Tuple2<String, Short>>> result = best.collect();
			result.forEach(System.out::println);*/

			JavaPairRDD<String, Tuple2<String, Integer>> union = emissions.join(emissions2006)
			 .mapToPair(c -> {
				short gdp2016 = c._2()._1();
				short gdp2006 = c._2()._2();
				float e = (gdp2016 - gdp2006)*100;
				int f = (int) (e/gdp2006);
				String country = c._1()._1();
				String continent = c._1()._2();
				return new Tuple2<>(continent, new Tuple2<>(country, f));
			 });

			//Parmis les pays lequels à fait le mieux ? de 2006 à 2016
			
			/*List<Object> res = union.take(20);
			res.forEach(System.out::println);*/

		JavaPairRDD<String, Tuple2<String, Integer>> best = union
				.reduceByKey((c1, c2) -> c1._2() > c2._2() ? c1 : c2)
		;
			
		 /*List<Object> result = union.collect();
		result.forEach(System.out::println);*/

		List<Tuple2<String, Tuple2<String, Integer>>> result = best.collect();
			result.forEach(System.out::println);

		}

		long s = (System.currentTimeMillis() - start) / 1000;
		String dur = String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60));
		System.out.println("Analysis completed in " + dur);
	}

	
}
