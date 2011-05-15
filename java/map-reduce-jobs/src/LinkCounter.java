import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class LinkCounter {
	
	static final String NAME = "linkcounter";
	
	static class LinkCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
		
		public static enum Counters {ROWS}
		
		@Override
		protected void map(ImmutableBytesWritable key, 
							Result values,
							Context context) throws IOException,InterruptedException {
			
			for (KeyValue value : values.list()) {
				if (value.getValue().length > 0) {
					context.getCounter(Counters.ROWS).increment(1);
					break;
				}
			}
			
		}
		
	}
	

	public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
		String tableName = "links";
		
		Job job = new Job(conf, NAME);
		job.setJarByClass(LinkCounter.class);
		
		Scan scan = new Scan();
		scan.setFilter(new FirstKeyOnlyFilter());
		scan.addColumn(Bytes.toBytes("core"), Bytes.toBytes("link"));
		
		job.setOutputFormatClass(NullOutputFormat.class);
		
		TableMapReduceUtil.initTableMapperJob(tableName, scan, LinkCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
		job.setNumReduceTasks(0);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = (new GenericOptionsParser(conf,args)).getRemainingArgs();
		if (otherArgs.length < 1) {
		}
		
		Job job = createSubmittableJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
