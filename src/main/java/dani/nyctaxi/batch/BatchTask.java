package dani.nyctaxi.batch;

import org.apache.spark.api.java.JavaRDD;

import dani.nyctaxi.TripDataItem;

/**
 * Implementations of this interface do batch analytics calculations on the
 * prepared NY City taxi trips data. The getResultRDD method returns the RDD
 * holding the results of this calculation. The input RDD contains instances of
 * TripDataItem class. The returned RDD may contain instances of an arbitrary
 * result class. The getResultItemClass returns a class object instance of the
 * items in the result RDD.
 *
 * @param <T>
 *            is the class of instances in the result RDD
 */
public interface BatchTask<T> {

	/**
	 * Created the RDD holding the final outcome of a batch analytics task
	 * 
	 * @param input
	 *            the RDD of trip data items
	 * @return RDD of the result of the batch analysis
	 */
	JavaRDD<T> getResultRDD(JavaRDD<TripDataItem> input);

	/**
	 * Returns result classes that are required by the BatchTaskRunner when
	 * saving the RDD
	 * 
	 * @return class of of the items hold in the RDD returned by the above
	 *         method.
	 */
	Class<T> getResultItemClass();
}