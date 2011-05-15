require 'open-uri'
def download_feed(full_url,name)
  output_path = File.join(File.dirname(__FILE__),"feeds","#{name}.xml")
  
  open(full_url) do |istream|
    open(output_path,"wb") do |ostream|
      ostream.write(istream.read)
    end
  end    
  
  puts "Downloaded #{full_url} to #{output_path}"
end

download_feed "http://weeklyreddit.appspot.com/rss/science", "reddit-science"
download_feed "http://weeklyreddit.appspot.com/rss/technology", "reddit-technology"
download_feed "http://weeklyreddit.appspot.com/rss/worldnews", "reddit-worldnews"
download_feed "http://news.ycombinator.com/rss", "hacker-news"
