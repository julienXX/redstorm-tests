require "redis"
require "mysql2"

mysql = Mysql2::Client.new(host: "localhost", username: "root", database: "wall_prod_clone")

redis = Redis.new(host: "localhost", port: 6379, db: 9)

messages = mysql.query("SELECT fb_author_id FROM fbitems LIMIT 10000")
count = messages.count

puts "- Messages: #{messages.count}"

i = 1
messages.each do |message|
  puts "\t processing #{message['fb_author_id']} (#{i}/#{count})"
  redis.rpush('author', message['fb_author_id'])
  i += 1
end
