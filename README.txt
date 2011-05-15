#setup rvm/jruby
rvm install jruby
rvm jruby

#run scripts
ruby download_latest_feeds.rb
ruby create_tables.rb
ruby populate_links_from_feeds.rb
ruby show_data.rb
