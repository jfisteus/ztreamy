from __future__ import print_function
from __future__ import unicode_literals

import sys
import urllib2
import time
import random
import gzip
import json
from rdflib import Graph, Namespace, Literal, URIRef, BNode

import ztreamy
#from ztreamy import rdfevents

seen_posts = set()
source_id = ztreamy.random_id()
application_id = 'identi.ca dataset'
identica_url = 'http://identi.ca/api/statuses/public_timeline.as'

ns_geo = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
ns_webtlab = Namespace("http://webtlab.it.uc3m.es/ns/")
ns_dc = Namespace("http://purl.org/dc/elements/1.1/")
ns_foaf = Namespace('http://xmlns.com/foaf/0.1/')
ns_rdf = Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')

uri_content = ns_webtlab['content']
uri_sent_to = ns_webtlab['sent_to']
uri_conversation = ns_webtlab['conversation']
uri_reply_to = ns_webtlab['reply_to']
uri_hashtag = ns_webtlab['hashtag']
uri_location = ns_webtlab['location']
uri_date = ns_dc['date']
uri_creator = ns_dc['creator']
uri_foaf_name = ns_foaf['name']
uri_longitude = ns_geo['long']
uri_latitude = ns_geo['lat']
uri_place = ns_geo['Place']
uri_based_near = ns_foaf['based_near']
uri_type = ns_rdf['type']

uri_tag = 'http://activityschema.org/object/hashtag'

def identica_download():
    try:
        data = urllib2.urlopen(identica_url)
        json_message = json.load(data)
    except:
        json_message = None
        print('warning: the HTTP request failed. Continuing...',
              file=sys.stderr)
    return json_message

def _add_location(data, subject_uri, predicate_uri, graph):
    if 'location' in data:
        location = data['location']
        place = BNode()
        graph.add((subject_uri, predicate_uri, place))
        graph.add((place, uri_type, uri_place))
        graph.add((place, uri_latitude, Literal(location['lat'])))
        graph.add((place, uri_longitude, Literal(location['lon'])))
    elif 'geopoint' in data:
        location = data['geopoint']['coordinates']
        place = BNode()
        graph.add((subject_uri, predicate_uri, place))
        graph.add((place, uri_type, uri_place))
        graph.add((place, uri_latitude, Literal(location[0])))
        graph.add((place, uri_longitude, Literal(location[1])))

def process_post(post):
    if (not 'url' in post
        or not 'actor' in post
        or not 'id' in post['actor']
        or not 'displayName' in post['actor']
        or not 'object' in post
        or not 'displayName' in post['object']):
        return None
    graph = Graph()
    graph.bind('geo', ns_geo)
    graph.bind('webtlab', ns_webtlab)
    graph.bind('dc', ns_dc)
    graph.bind('rdf', ns_rdf)
    graph.bind('foaf', ns_foaf)
    post_id = URIRef(post['url'])
    author_id = Literal(post['actor']['id'])
    graph.add((post_id, uri_date, Literal(post['published'])))
    graph.add((post_id, uri_creator, author_id))
    graph.add((author_id, uri_foaf_name,
               Literal(post['actor']['displayName'])))
    graph.add((post_id, uri_content, Literal(post['object']['displayName'])))

    if 'to' in post and 'url' in post['to']:
        graph.add((post_id, uri_sent_to, URIRef(post['to']['url'])))
    if 'context' in post:
        context = post['context']
        if 'conversation' in context:
            graph.add((post_id, uri_conversation,
                       Literal(context['conversation'])))
        if 'inReplyTo' in context:
            graph.add((post_id, uri_reply_to,
                       Literal(context['inReplyTo']['url'])))
    if 'tags' in post['object']:
        for tag in post['object']['tags']:
            if tag['objectType'] == uri_tag:
                graph.add((post_id, uri_hashtag, Literal(tag['displayName'])))
    _add_location(post, post_id, uri_location, graph)
    _add_location(post['actor'], author_id, uri_based_near, graph)

    event = ztreamy.RDFEvent(source_id, 'text/n3', graph,
                             application_id=application_id)
    return event

def process_download(data, max_num_posts):
    events = []
    num_posts = 0
    for post in data['items']:
        if (not 'verb' in post or post['verb'] != 'post'
            or not 'object' in post or not 'id' in post['object']):
            continue
        try:
            post_id = post['object']['id']
        except:
            print(repr(post))
            raise
        if not post_id in seen_posts:
            seen_posts.add(post_id)
            event = process_post(post)
            if event is not None:
                events.append(event)
            num_posts += 1
            if num_posts == max_num_posts:
                break
    return events

def randomize_timestamps(events, interval_duration):
    current_time = ztreamy.parse_timestamp(events[0].timestamp)
    time_max = current_time + interval_duration - 1
    exp_rate = 1.3 * len(events) / interval_duration
    for event in events:
        current_time += random.expovariate(exp_rate)
        timestamp = int(current_time)
        if timestamp > time_max:
            timestamp = time_max
        event.timestamp = ztreamy.get_timestamp(date=timestamp)

def loop(delay, num_posts, file_):
    while len(seen_posts) < num_posts:
        data = identica_download()
        if data is not None:
            new_events = process_download(data, num_posts - len(seen_posts))
            if new_events:
                randomize_timestamps(new_events, delay)
            for event in new_events:
                file_.write(str(event))
        print('Gathered {0} posts'.format(len(seen_posts)), file=sys.stderr)
        time.sleep(delay)

def main():
    if len(sys.argv) != 4:
        print('Arguments expected: period (float in seconds), num. posts,'
              ' output file',
              file=sys.stderr)
        print('e.g. python download_identica 30.0 50 events.txt.gz',
              file=sys.stderr)
    else:
        output = gzip.GzipFile(sys.argv[3], 'w')
        try:
            loop(float(sys.argv[1]), int(sys.argv[2]), output)
        except KeyboardInterrupt:
            pass
        finally:
            output.close()

if __name__ == '__main__':
    main()
