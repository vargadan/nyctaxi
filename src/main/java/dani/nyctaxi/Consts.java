package dani.nyctaxi;

import java.net.Inet4Address;
import java.time.LocalDateTime;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public abstract class Consts {

	static {
		String hadoopURI = null;
		try {
			hadoopURI = "hdfs://" + Inet4Address.getLocalHost().getHostAddress()
					+ ":9010/data/*";
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			HADOOP_INPUT_FILE = hadoopURI;
		}
	}

	public static String AWS_ACCESS_KEY = System.getenv("AWS_ACCESS_KEY_ID");

	public static String AWS_SECRET_KEY = System
			.getenv("AWS_SECRET_ACCESS_KEY");

	public static final String S3_OUTPUT_ROOT = "s3a://nyctaxi-output/"
			+ LocalDateTime.now().toString() + "/";

	public static final String S3_INPUT_FILE = "s3a://nyctaxi-input-split/*";

	public static final String HADOOP_INPUT_FILE;

	public static final String S3_INPUT_FILE_PARTIAL = "s3n://nyctaxi-input/partial_sorted_data.csv";

	public static final String LOCAL_INPUT_FILE = "/Users/dani/temp/sorted_data.csv";
	// public static final String LOCAL_INPUT_FILE =
	// "/Users/dani/temp/nyctaxi-sample.csv";

	public static final String LOCAL_OUTPUT_ROOT = "/Users/dani/temp/nyctaxi-output/"
			+ LocalDateTime.now().toString() + "/";

	public static StructType INPUT_SCHEMA = new StructType()
			.add("vehicle", DataTypes.StringType)
			.add("driver", DataTypes.StringType)
			.add("pickup_datetime_s", DataTypes.StringType)
			.add("dropoff_datetime_s", DataTypes.StringType)
			.add("trip_time_in_secs", DataTypes.IntegerType)
			.add("trip_distance", DataTypes.DoubleType)
			.add("pickup_longitude", DataTypes.DoubleType)
			.add("pickup_latitude", DataTypes.DoubleType)
			.add("dropoff_longitude", DataTypes.DoubleType)
			.add("dropoff_latitude", DataTypes.DoubleType)
			.add("payment_type", DataTypes.StringType)
			.add("fare_amount", DataTypes.DoubleType)
			.add("surcharge", DataTypes.DoubleType)
			.add("mta_tax", DataTypes.DoubleType)
			.add("tip_amount", DataTypes.DoubleType)
			.add("tolls_amount", DataTypes.DoubleType)
			.add("total_amount", DataTypes.DoubleType);

}
