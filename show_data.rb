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
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.io.Text

conf = HBaseConfiguration.new

table = HTable.new(conf,"links")

scan = Scan.new
scanner = table.get_scanner(scan)
begin
  while (row = scanner.next()) do
    row_key = Bytes.toString(row.getRow())
    link = Bytes.toString(row.getValue(Bytes.toBytes("core"),Bytes.toBytes("link")))
    
    last_processed_bytes = row.getValue(Bytes.toBytes("core"),Bytes.toBytes("lastProcessed"))
    last_processed = last_processed_bytes.nil? ? nil : Bytes.toLong(last_processed_bytes)
    
    title_bytes = row.getValue(Bytes.toBytes("meta"),Bytes.toBytes("title"))
    title = title_bytes.nil? ? nil : Bytes.toString(title_bytes)
    
    p [row_key,link,title,last_processed]
  end
ensure
  scanner.close
end


