require 'rubygems'
require 'simple-rss'
require 'open-uri'

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
import org.apache.hadoop.io.Text

add_counter = 0
conf = HBaseConfiguration.new
table = HTable.new(conf,"links")

Dir.glob(File.join(File.dirname(__FILE__),"feeds","*.xml")).each do |feed_path|
  
  puts "***** START OF #{feed_path} *****"  
  
  open(feed_path,"r") do |istream|
    rss = SimpleRSS.parse(istream)
    rss.items.each do |itm|
      
      link = itm.link.strip
      
      next if link =~ /reddit\.com/i #ignore reddit self-posts
      next if link =~ /imgur\.com/i #ignore image links
      next if link =~ /youtube\.com/i #ignore youtube links
      next if link =~ /github\.com/i #ignore github links
      next if link =~ /\.(gif|jpg|jpeg|png)$/i #ignore image links
      
      #add the link
      key = Bytes.toBytes("sample#{add_counter}")
      family = Bytes.toBytes("core")
      column = Bytes.toBytes("link")
      value = Bytes.toBytes(link)
      table.put(Put.new(key).add(family,column,value))
      puts "added sample#{add_counter}, #{link}"
      add_counter += 1      
      
    end
  end
  
  puts "***** END OF #{feed_path} *****"
end




