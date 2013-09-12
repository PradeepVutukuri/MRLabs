package customsort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import customsort.DividendJob.DividendGrowthMapper;
import customsort.DividendJob.DividendGrowthReducer;

public class DividendJobTest {

	private MapDriver<LongWritable, Text, Stock, DoubleWritable> mapDriver;
	private ReduceDriver<Stock, DoubleWritable, NullWritable, DividendChange> reduceDriver;
	private DividendGrowthReducer reducer;
	
	@Before
	public void setup() {
		DividendGrowthMapper mapper = new DividendGrowthMapper();
		reducer = new DividendGrowthReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}
	
	@Test
	public void testMapper() {
		LongWritable inputKey = new LongWritable(0);
		Text inputValue = new Text("NYSE,APX,2009-09-11,0.047");
		Stock outputKey = new Stock("APX", "2009-09-11");
		DoubleWritable outputValue = new DoubleWritable(0.047);
		mapDriver.withInput(inputKey, inputValue);
		mapDriver.withOutput(outputKey, outputValue);
/*		try {
			List<Pair<Stock, DoubleWritable>> results = mapDriver.run();
			for(Pair<Stock, DoubleWritable> result : results) {
				Stock stock = result.getFirst();
				double dividend = result.getSecond().get();
				System.out.println(stock.toString() + " " + dividend);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
*/
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		String symbol = "ADC";
		reducer.previousStock = new Stock(symbol, "2009-06-26");
		reducer.previousPrice = 0.5;
		reduceDriver.setInputKey(new Stock(symbol, "2009-09-28"));
		reduceDriver.addInputValue(new DoubleWritable(0.51));
		reduceDriver.withOutput(NullWritable.get(), new DividendChange(symbol, "2009-09-28",0.01));
		reduceDriver.runTest();
	}
}

