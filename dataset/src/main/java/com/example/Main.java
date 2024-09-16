package com.example;

import static com.example.utils.Utils.*;

import com.example.utils.CloseableIterator;
import java.io.File;
import java.net.URL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.Column;
//import org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.*;
import java.io.Serializable;

public class Main {

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("dataset");

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
		File inFile = new File("idh.csv");
		downloadIfAbsent(new URL("https://svn.ensisa.uha.fr/bd/co2/owid-co2-data-continents.csv"), inFile);
		try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate()) {
			Dataset<Row> raw = spark.read().format("csv")
     			.option("sep", ",")
     			.option("header", "true")
    			.option("encoding", "UTF-8")
    			.load(inFile.getAbsolutePath());
			
		   /*Dataset<Row> impr = raw
			   .select(col("continent"), col("country"), col("2017").minus(col("2007")).divide(col("2007")).as("Improvement"))
			   .where(col("2007").isNotNull())
			   ;*/
			   Dataset<Row> impr2016 = raw
			   .select(col("country"), col("continent"),col("gdp").divide(col("total_ghg")).as("rapport 2016"))
			   .filter (col("year").equalTo(2016));

			   Dataset<Row> impr2006 = raw
			   .select(col("continent").as("continent2"), col("country").as("country2"), col("gdp").divide(col("total_ghg")).as("rapport 2006"))
			   .filter (col("year").equalTo(2006));
			  
			   impr2006.show();

			   Dataset<Row> join = impr2006.join(impr2016, impr2006.col("country").equalTo(impr2016.col("country2")))
			   .select(col("continent"),col("country"), col("rapport 2006").minus(col("rapport 2016")));
			   
			join.show();
		   /*
		   Dataset<Row> result;
		   // Missing country name as it's lost by the aggregating function:
		   //result = impr.groupBy("Continent").max("Improvement");
		   result = impr
			   // Adding a new column showing max Improvement of a partition ; the partition is the subset of data that has same 
			   // The new column is added for each country
			   .withColumn("max Impr", max(col("Improvement")).over(Window.partitionBy(col("Continent"))))
			   .where(col("max Impr").equalTo(col("Improvement"))) // We only require the max one
			   .drop("max Impr") // No longer need for this additional column
			   ;
			   
			result.show();*/
		
		}

		long s = (System.currentTimeMillis() - start) / 1000;
		String dur = String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60));
		System.out.println("Analysis completed in " + dur);
	}
}
