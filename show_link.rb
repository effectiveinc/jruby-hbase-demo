include Java

Dir.glob(File.join(File.dirname(__FILE__),"javalib","*.jar")).each do |jar_path|
  require jar_path
end

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.io.Text

raise "Must include row_key as first arg" unless ARGV.length >= 1
row_key = Bytes.toBytes(ARGV[0])

conf = HBaseConfiguration.new
table = HTable.new(conf,"links")
get = Get.new(row_key)
row = table.get(get)

link = Bytes.toString(row.getValue(Bytes.toBytes("core"),Bytes.toBytes("link")))

last_processed_bytes = row.getValue(Bytes.toBytes("core"),Bytes.toBytes("lastProcessed"))
last_processed = last_processed_bytes.nil? ? nil : Bytes.toLong(last_processed_bytes)

summary_bytes = row.getValue(Bytes.toBytes("meta"),Bytes.toBytes("summary"))
summary = summary_bytes.nil? ? nil : Bytes.toString(summary_bytes)

title_bytes = row.getValue(Bytes.toBytes("meta"),Bytes.toBytes("title"))
title = title_bytes.nil? ? nil : Bytes.toString(title_bytes)

puts "Link #{row_key}: #{link}"
puts "Title: #{title}"
puts "Last Processed: #{last_processed}"
puts "Summary: \n#{summary}"
