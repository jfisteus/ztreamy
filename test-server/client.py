import tornado.ioloop
import tornado.httpclient

def handle_request(response):
    if response.error:
        print "Error:", response.error
    else:
        print response.body
    tornado.ioloop.IOLoop.instance().stop()

def data_callback(data):
    print data

url = 'http://localhost:8888/events/stream'
http_client = tornado.httpclient.AsyncHTTPClient()
request = tornado.httpclient.HTTPRequest(url, streaming_callback=data_callback,
                                         request_timeout=None,
                                         connect_timeout=None)
http_client.fetch(request, handle_request)
tornado.ioloop.IOLoop.instance().start()
