package com.example;

import static com.example.utils.Utils.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import scala.Tuple10;
import scala.Tuple11;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.streaming.Duration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;


import com.example.utils.Utils;





public class Main {
	public static final String APP_NAME = "kickoff";
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

		File inFile = new File("data.csv");
		Utils.downloadIfAbsent(new URL("https://svn.ensisa.uha.fr/bd/co2/energy-consumption-by-source-and-region.csv"), inFile);

		try (JavaSparkContext sc = new JavaSparkContext(conf)) {

			JavaRDD<String[]> raw = sc.textFile(inFile.getAbsolutePath(), 32)
				.map(l -> fromCSV(l, ','))
				.filter(a -> a.length > 0)
				;

				final String[] headers = raw.first();
			final Map<String, Integer> h = new TreeMap<>();
			for(int i = 0; i < headers.length; ++i) {
				h.put(headers[i], i);
			}

			raw = raw.filter(a -> !Arrays.equals(a, headers))
			;

			//Question 1

			JavaRDD<Tuple11<String, String, String, String, String, String, String, String, String, String, String>> tabSumAll = raw.map(a -> new Tuple11<>(a[h.get("Year")], a[h.get("Entity")],a[h.get("Geo Biomass Other")],a[h.get("Biofuels")],a[h.get("Solar")],a[h.get("Wind")],a[h.get("Hydro")], a[h.get("Nuclear")],a[h.get("Gas")], a[h.get("Oil")],a[h.get("Coal")]))
			.filter(t -> !t._1().isEmpty() && !t._2().isEmpty() && !t._3().isEmpty() && !t._4().isEmpty() && !t._5().isEmpty()&& !t._6().isEmpty()&& !t._7().isEmpty()&& !t._8().isEmpty()&& !t._9().isEmpty() && !t._10().isEmpty()&& !t._11().isEmpty())
			;

			JavaRDD<Tuple3<Tuple2<String/*Years */,String /*Country */>,Float /*Sum of oil and coal */,Float /*Sum of all energies */>> sumAll = tabSumAll.map(c ->{
				float geo = Float.parseFloat(c._3());
				float biofuels = Float.parseFloat(c._4());
				float solar = Float.parseFloat(c._5());
				float wind = Float.parseFloat(c._6());
				float hydro = Float.parseFloat(c._7());
				float nuclear = Float.parseFloat(c._8());
				float gas = Float.parseFloat(c._9());
				float oil = Float.parseFloat(c._10());
				float coal = Float.parseFloat(c._11());

				float fsumCO = oil+ coal;
				float fsumAll = geo+ biofuels+ solar+ wind+ hydro+ nuclear+ gas+ coal + oil;
				return new Tuple3<>(new Tuple2<>(c._1(), c._2()), fsumCO, fsumAll);
			});


			JavaRDD<Tuple4<String/*Years */,String /*Country */,Float /*Sum of oil and coal */,Float /*Sum of all energies */>> sumAllTest = tabSumAll.map(c ->{
				float geo = Float.parseFloat(c._3());
				float biofuels = Float.parseFloat(c._4());
				float solar = Float.parseFloat(c._5());
				float wind = Float.parseFloat(c._6());
				float hydro = Float.parseFloat(c._7());
				float nuclear = Float.parseFloat(c._8());
				float gas = Float.parseFloat(c._9());
				float oil = Float.parseFloat(c._10());
				float coal = Float.parseFloat(c._11());

				float fsumCO = oil+ coal;
				float fsumAll = geo+ biofuels+ solar+ wind+ hydro+ nuclear+ gas+ coal + oil;
				return new Tuple4<>(c._1(), c._2(), fsumCO, fsumAll);
			}).groupByKey("Years");






			//Le résultat obtenu sera ((Années,Pays), Somme du charbon et du pétrol, Somme de toute les énergies)
			List<Tuple3<Tuple2<String, String>, Float, Float>> result = sumAll.collect();

			result.forEach(System.out::println);


			//Question 2 (year, name of enegie, total energie)



/*
			totalSum= tabSumAll.flatMap(t ->
			{Iterator<String> iterator;
				if (iterator.hasNext()){
					t.
				}

			});*/


		}

		long s = (System.currentTimeMillis() - start) / 1000;
		String dur = String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60));
		System.out.println("Analysis completed in " + dur);
	}

}
