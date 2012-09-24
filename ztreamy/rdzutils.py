from rdz.rdzstream import StreamCompressor, StreamDecompressor

from events import Deserializer
from rdfevents import RDFEvent
from ztreamy import ZtreamyException

class EventCompressor(StreamCompressor):
    def __init__(self, **kwargs):
        super(EventCompressor, self).__init__(**kwargs)

    def compress(self, events):
        data = []
        for event in events:
            if isinstance(event, RDFEvent):
                self.new_text_part()
                data.extend(self.compress_text(event.serialize_headers()))
                self.new_graph_part()
                data.extend(self.compress_graph(event.body))
            else:
                self.new_text_part()
                data.extend(self.compress_text(event.serialize_headers()))
                self.new_text_part()
                data.extend(self.compress_text(event.serialize_body()))
        return ''.join(data)


class EventDecompressor(StreamDecompressor):
    def __init__(self, **kwargs):
        super(EventDecompressor, self).__init__(**kwargs)
        self.parts = []

    def decompress(self, data):
        events = []
        self.parts.extend(super(EventDecompressor, self).decompress(data))
        for header_part, body_part in zip(self.parts[::2], self.parts[1::2]):
            header = header_part.text
            assert header is not None
            headers, extra_headers = Deserializer.deserialize_headers(header)
            if body_part.finished:
                if body_part.is_graph():
                    body = body_part.graph
                elif body_part.is_text():
                    body = body_part.text
                else:
                    raise ZtreamyException('Bad part type for event body')
                assert body is not None
                events.append(Deserializer.create_event(headers,
                                                        extra_headers, body))
            else:
                self.parts.remove(body_part)
                break
        self.parts = self.parts[len(events) * 2:]
        return events
