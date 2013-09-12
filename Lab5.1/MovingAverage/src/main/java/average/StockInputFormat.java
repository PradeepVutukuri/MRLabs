package average;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class StockInputFormat extends FileInputFormat<Stock, StockPrices> {

	public static class StockReader extends RecordReader<Stock, StockPrices> {
		private Stock key = new Stock();
		private StockPrices value = new StockPrices();
		private BufferedReader in;
		private final String EXCHANGE = "exchange";
		
		@Override
		public void close() throws IOException {
			in.close();
		}

		@Override
		public Stock getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public StockPrices getCurrentValue() throws IOException,
				InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit) inputSplit;
			Configuration conf = context.getConfiguration();
			Path path = split.getPath();
			InputStream is = path.getFileSystem(conf).open(path);
			in = new BufferedReader(new InputStreamReader(is));
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			String line = in.readLine();
			if(line == null) return false;
			String [] words = StringUtils.split(line, ',');
			if(EXCHANGE.equals(words[0])) {
				line = in.readLine();
				words = StringUtils.split(line, ',');
			}
			key.setSymbol(words[1]);
			key.setDate(words[2]);
			value.setOpen(Double.parseDouble(words[3]));
			value.setHigh(Double.parseDouble(words[4]));
			value.setLow(Double.parseDouble(words[5]));
			value.setClose(Double.parseDouble(words[6]));
			value.setVolume(Integer.parseInt(words[7]));
			value.setAdjustedClose(Double.parseDouble(words[8]));
			
			return true;
		}

	}

	@Override
	public RecordReader<Stock, StockPrices> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new StockReader();
	}

}
