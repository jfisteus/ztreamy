from rdz.rdzstream import StreamCompressor, StreamDecompressor

from events import Deserializer
from rdfevents import RDFEvent

class EventCompressor(StreamCompressor):
    def __init__(self, **kwargs):
        super(EventCompressor, self).__init__(**kwargs)

    def compress(self, events):
        data = []
        for event in events:
            assert isinstance(event, RDFEvent)
            self.new_text_part()
            data.extend(self.compress_text(event.serialize_headers()))
            self.new_graph_part()
            data.extend(self.compress_graph(event.body))
        return ''.join(data)


class EventDecompressor(StreamDecompressor):
    def __init__(self, **kwargs):
        super(EventDecompressor, self).__init__(**kwargs)
        self.parts = []

    def decompress(self, data):
        print 'decompressing', len(data)
        events = []
        self.parts.extend(super(EventDecompressor, self).decompress(data))
        for header_part, body_part in zip(self.parts[::2], self.parts[1::2]):
            print header_part
            print body_part
            header = header_part.text
            body = body_part.graph
            if body_part.finished:
                assert header is not None
                assert body is not None
                headers, extra_headers = \
                         Deserializer.deserialize_headers(header)
                events.append(Deserializer.create_event(headers, extra_headers,
                                                        body))
            else:
                self.parts.remove(body_part)
                break
        self.parts = self.parts[len(events) * 2:]
        return events
