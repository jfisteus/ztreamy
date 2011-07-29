import tornado.ioloop
import tornado.httpclient

url = 'http://localhost:8888/events/next'
http_client = tornado.httpclient.AsyncHTTPClient()

def handle_request(response):
    if response.error:
        print "Error:", response.error
        tornado.ioloop.IOLoop.instance().stop()
    else:
        print response.body
    send_request(http_client, url)

def send_request(http_client, url):
    request = tornado.httpclient.HTTPRequest(url, request_timeout=None,
                                             connect_timeout=None)
    http_client.fetch(request, handle_request)

send_request(http_client, url)
tornado.ioloop.IOLoop.instance().start()
