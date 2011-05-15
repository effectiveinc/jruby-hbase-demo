import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.GregorianCalendar;

import javax.swing.text.html.HTMLDocument;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import de.l3s.boilerpipe.extractors.ArticleExtractor;


public class LinkMetaFetcher {
	
	static final String NAME = "linkmetafetcher";
	
	static class LinkMetaFetcherMapper extends TableMapper<ImmutableBytesWritable, Result> {
		
		public static enum Counters {ROWS}
		
		@Override
		protected void map(ImmutableBytesWritable key, 
							Result rowData,
							Context context) throws IOException,InterruptedException {
			

			byte[] linkBytes = rowData.getValue(Bytes.toBytes("core"), Bytes.toBytes("link"));
			byte[] lastProcessedBytes = rowData.getValue(Bytes.toBytes("core"), Bytes.toBytes("lastProcessed"));
			String link = Bytes.toString(linkBytes);
	
			if (lastProcessedBytes != null) return;
			
			try {
				
				String html = fetchLinkData(link);
				String summaryText = null;
				String titleText = null;
				
				try {
					summaryText = ArticleExtractor.INSTANCE.getText(html);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				try {
					Document doc = Jsoup.parse(html);
					for (Element e : doc.select("title")) {
						titleText = e.text();
						break;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				HTable table = new HTable(context.getConfiguration(),Bytes.toBytes("links"));

				if (summaryText != null) putValue(table, rowData.getRow(), "meta", "summary", summaryText);
				if (titleText != null) putValue(table, rowData.getRow(), "meta", "title", titleText);
				if (titleText != null || summaryText != null) putValue(table, rowData.getRow(), "core", "lastProcessed", Bytes.toBytes((new GregorianCalendar()).getTimeInMillis()));
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		protected String fetchLinkData(String link) throws IOException {
			URL url = new URL(link);

			URLConnection conn = url.openConnection();
			InputStream inputStream = null;
			String htmlText = null;
			
			try {
				inputStream = conn.getInputStream();
				htmlText = IOUtils.toString(conn.getInputStream());
			} finally {
				if (inputStream != null) {
					try {
						inputStream.close();
					} catch (IOException e) {}
				}
			}
			return htmlText;
		}
		
		

		protected void putValue(HTable table, byte[] rowKey, String family, String column, String value) throws Exception {
			putValue(table,rowKey, family,column,Bytes.toBytes(value));
		}
		
		protected void putValue(HTable table, byte[] rowKey, String family, String column, byte[] valueBytes) throws Exception {
			byte[] familyBytes = Bytes.toBytes(family);
			byte[] columnBytes = Bytes.toBytes(column);
			
			Put p = new Put(rowKey);
			p.add(familyBytes,columnBytes,valueBytes);
			table.put(p);
		}
		
		
	}
	

	public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
		String tableName = "links";
		
		Job job = new Job(conf, NAME);
		job.setJarByClass(LinkMetaFetcher.class);
		
		Scan scan = new Scan();
		//scan.setFilter(new FirstKeyOnlyFilter());
		scan.addColumn(Bytes.toBytes("core"), Bytes.toBytes("link"));
		scan.addColumn(Bytes.toBytes("core"), Bytes.toBytes("lastProcessed"));
		
		job.setOutputFormatClass(NullOutputFormat.class);
		
		TableMapReduceUtil.initTableMapperJob(tableName, scan, LinkMetaFetcherMapper.class, ImmutableBytesWritable.class, Result.class, job);
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
