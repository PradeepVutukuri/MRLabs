package hbase;

import java.io.IOException;
import static hbase.StockConstants.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class StockImporter extends Configured implements Tool {

	public static class StockImporterMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
		private final String COMMA = ",";
		private HTable htable;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			htable = new HTable("stocks");
			htable.setAutoFlush(false);
			htable.setWriteBufferSize(200000);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] words = value.toString().split(COMMA);
			if(words[0].equals("exchange")) return;
			
			String symbol = words[1];
			String date = words[2];
			double highPrice = Double.parseDouble(words[4]);
			double lowPrice = Double.parseDouble(words[5]);
			double closingPrice = Double.parseDouble(words[6]);
			double volume = Double.parseDouble(words[7]);
			
			byte [] stockRowKey = Bytes.add(date.getBytes(), symbol.getBytes());
			Put put = new Put(stockRowKey);
			put.add(PRICE_COLUMN_FAMILY, HIGH_QUALIFIER, Bytes.toBytes(highPrice));
			put.add(PRICE_COLUMN_FAMILY, LOW_QUALIFIER, Bytes.toBytes(lowPrice));
			put.add(PRICE_COLUMN_FAMILY, CLOSING_QUALIFIER, Bytes.toBytes(closingPrice));
			put.add(PRICE_COLUMN_FAMILY, VOLUME_QUALIFIER, Bytes.toBytes(volume));
			htable.put(put);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			htable.close();
		}
	}


	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "StockImportJob");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(StockImporter.class);
		
		Path out = new Path("temp");
		out.getFileSystem(conf).delete(out,true);
		FileInputFormat.setInputPaths(job, new Path("stocksA"));
		FileOutputFormat.setOutputPath(job, out);
		
		job.setMapperClass(StockImporterMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(0);
		
		TableMapReduceUtil.addDependencyJars(job);
		
		return job.waitForCompletion(true)?0:1;

	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new StockImporter(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}
}
