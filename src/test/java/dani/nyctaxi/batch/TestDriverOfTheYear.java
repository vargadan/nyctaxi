package dani.nyctaxi.batch;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;

public class TestDriverOfTheYear {
	

	  private JavaSparkContext sc;

	  @Before
	  public void setUp() {
	    sc = new JavaSparkContext("local", "SparkedTests");
	  }

	  @After
	  public void tearDown() {
	    sc.stop();
	    sc = null;
	  }
	  

}
