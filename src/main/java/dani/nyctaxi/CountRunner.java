package dani.nyctaxi;

import static dani.nyctaxi.Consts.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class CountRunner {
	
	private static SparkConf sc;
	
	private static JavaSparkContext jsc;
	
	private static SQLContext sqlContext;


	public static void main(String... args) {

		{
			sc = new SparkConf().setAppName(CountRunner.class.getName());
			jsc = new JavaSparkContext(sc);
			sqlContext = new SQLContext(jsc);
			
			jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY);
			jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_KEY);
		}

		Instant startX = Instant.now();
		
		long count_onePiece = 0;
		long count_hadoop = 0;
		long count_distint = 0;

		try {
			
			try {
				DataFrame inputDF_split = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "false")
						.option("header", "false")
						.schema(INPUT_SCHEMA)
						.load(HADOOP_INPUT_FILE);	
				count_hadoop = inputDF_split.count();
			} catch (Exception e) {
				e.printStackTrace();
			} 

//			try {
//				DataFrame inputDF_onePiece = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "false")
//					.option("header", "false")
//					.schema(inputSchema)
//					.load(S3_INPUT_FILE);
//				count_onePiece = inputDF_onePiece.count();
//			} catch (Exception e) {
//				e.printStackTrace();
//			} 		
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			Instant endX = Instant.now();
			
			long duration = ChronoUnit.SECONDS.between(startX, endX);
			System.out.println("* * * * * *");
			System.out.println("...was running for " + duration + " seconds.");
			System.out.println("count_onePiece : " + count_onePiece);
			System.out.println("count_hadoop    : " + count_hadoop);
			System.out.println("* * * * * *");
			jsc.stop();
		}
		
	}
	

}