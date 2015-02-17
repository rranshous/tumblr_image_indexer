require 'sinatra'
require 'thread_safe'
require 'eventstore'
require 'base64'

# we want to be able to answer the question
#  what blog + post did this image come from?

image_to_post = ThreadSafe::Cache.new
post_to_blog = ThreadSafe::Cache.new
blog_to_posts = ThreadSafe::Cache.new
post_to_images = ThreadSafe::Cache.new

IMAGE_STASHER_URL = ENV['IMAGE_STASHER_URL']

CONNSTRING = ENV['EVENTSTORE_URL'] || 'http://0.0.0.0:2113'
eventstore = EventStore::Client.new(CONNSTRING)

def events_with_sleep eventstore, stream, start_at, set_size, sleep_time=10
  Enumerator.new do |yielder|
    start_at = 0
    last_start_at = nil
    begin
      loop do
        if last_start_at == start_at
          sleep sleep_time
        end
        last_start_at = start_at
        events = eventstore.resume_read(stream, start_at, set_size)
        events.each do |event|
          yielder << event
          start_at = event[:id]
        end
      end
    end
  end
end

Thread.abort_on_exception = true

Thread.new do
  events_with_sleep(eventstore, 'new-images', 0, 100).each do |event|
    puts "image EVENT: #{event[:body]['href']}"
    image_to_post[event[:body]['href']] = event[:body]['post']['href']
    (post_to_images[event[:body]['post']['href']] ||= []) << event[:body]['href']
  end
end

Thread.new do
  events_with_sleep(eventstore, 'new-posts', 0, 100).each do |event|
    post_to_blog[event[:body]['href']] = event[:body]['blog']['href']
    (blog_to_posts[event[:body]['blog']['href']] ||= []) << event[:body]['href']
  end
end

get '/' do
  etag image_to_post.keys.length
  image_to_post.keys.map do |image_name|
    "http://#{request.host}/#{Base64.urlsafe_encode64(image_name)}"
  end.to_json
end

get '/links' do
  etag image_to_post.keys.length
  content_type :html
  image_to_post.keys.map do |image_name|
    "http://#{request.host}:#{request.port}/#{Base64.urlsafe_encode64(image_name)}"
  end.reduce("") do |acc, url|
    acc + "<a href='#{url}/html'>#{url}/html</a><br/>"
  end
end

get '/browse' do
  content_type :html
  r = """
  <style>
    .blog { margin-left: 10px; }
    .post { margin-left: 20px; }
    .image { margin-left: 30px; }
  </style>
  """
  blog_to_posts.keys.each do |blog|
    r += "<div class='blog'><a href='#{blog}'>#{blog}</a></div>"
    blog_to_posts[blog].each do |post|
      r += "<div class='post'><a href='#{post}'>#{post}</a></div>"
      if post_to_images[post].nil?
        puts "MISSING: #{post}"
        next
      end
      post_to_images[post].each do |image|
        url = "http://#{request.host}:#{request.port}/#{Base64.urlsafe_encode64(image)}/html"
        r += "<div class='image'><a href='#{url}'>#{image}</a></div>"
      end
    end
  end
  r
end

get '/:image_name_encoded/html' do |image_href_encoded|
  image_href = Base64.urlsafe_decode64 image_href_encoded
  content_type :html
  puts "HREF: #{image_href}"
  post_href = image_to_post[image_href]
  blog_href = post_to_blog[post_href]
  """
  <h1>#{image_href}</h1>
  <h2>#{post_href}</h2>
  <h3>#{blog_href}</h3>
  <img src='#{IMAGE_STASHER_URL}/#{image_href_encoded}'/>
  """
end

get '/favicon.ico' do
  ''
end

get '/:image_name_encoded' do |image_href_encoded|
  image_href = Base64.urlsafe_decode64 image_href_encoded
  content_type :json
  puts "HREF: #{image_href}"
  post_href = image_to_post[image_href]
  blog_href = post_to_blog[post_href]
  { href: image_href, post: { href: post_href }, blog: { href: blog_href }}.to_json
end

