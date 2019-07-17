import codecs

__all__ = ['SerializerNotInstalled', 'registry']


class SerializerNotInstalled(Exception):
    """Support for the requested serialization type is not installed"""


class SerializerRegistry(object):
    """The registry keeps track of serialization methods."""

    def __init__(self):
        self._encoders = {}
        self._decoders = {}
        self._default_encode = None
        self._default_content_type = None
        self._default_content_encoding = None

    def register(self, name, encoder, decoder, content_type,
                 content_encoding='utf-8'):
        """Register a new encoder/decoder.
        """
        if encoder:
            self._encoders[name] = (content_type, content_encoding, encoder)
        if decoder:
            self._decoders[content_type] = decoder

    def _set_default_serializer(self, name):
        """
        Set the default serialization method used by this library.
        """
        try:
            (self._default_content_type, self._default_content_encoding,
             self._default_encode) = self._encoders[name]
        except KeyError:
            raise SerializerNotInstalled("No encoder installed for %s" % name)

    def encode(self, data, serializer=None):
        """
        Serialize a data structure into a string suitable for sending
        """
        if serializer == "raw":
            return raw_encode(data)

        if serializer and not self._encoders.get(serializer):
            raise SerializerNotInstalled("No encoder installed for %s" % serializer)

        if not serializer and isinstance(data, str):
            return "application/data", "binary", data

        if not serializer and isinstance(data, bytes):
            payload = data.encode('utf-8')
            return "text/plain", "utf-8", payload

        if serializer:
            content_type, content_encoding, encoder = self._encoders[serializer]
        else:
            encoder = self._default_encode
            content_type = self._default_content_type
            content_encoding = self._default_content_encoding

        payload = encoder(data)
        return content_type, content_encoding, payload

    def decode(self, data, content_type, content_encoding):
        """Deserialize a data stream as serialized using ``encode``
        """
        content_type = content_type or 'application/json'
        content_encoding = (content_encoding or 'utf-8').lower()

        if content_encoding not in ('binary', 'ascii-8bit') and not isinstance(data, str):
            data = codecs.decode(data, content_encoding)
        
        try:
            decoder = self._decoders[content_type]
        except KeyError:
            return data

        return decoder(data)


def raw_encode(data):
    """Special case serializer."""
    content_type = 'application/data'
    payload = data
    if isinstance(payload, str):
        content_encoding = 'utf-8'
        payload = payload.encode(content_encoding)
    else:
        content_encoding = 'binary'
    return content_type, content_encoding, payload


def register_json():
    """Register a encoder/decoder for JSON serialization."""

    # Try to import a module that provides json parsing and emitting, starting
    # with the fastest alternative and falling back to the slower ones.
    try:
        # cjson is the fastest
        import cjson
        json_serialize = cjson.encode
        json_deserialize = cjson.decode
    except ImportError:
        try:
            # Then try to find simplejson. Later versions has C speedups which
            # makes it pretty fast.
            import simplejson
            json_serialize = simplejson.dumps
            json_deserialize = simplejson.loads
        except ImportError:
            try:
                # Then try to find the python 2.6 stdlib json module.
                import json
                json_serialize = json.dumps
                json_deserialize = json.loads
            except ImportError:
                # If all of the above fails, fallback to the simplejson
                # embedded in Django.
                from django.utils import simplejson
                json_serialize = simplejson.dumps
                json_deserialize = simplejson.loads

    registry.register('json', json_serialize, json_deserialize,
                      content_type='application/json',
                      content_encoding='utf-8')


def register_yaml():
    """Register a encoder/decoder for YAML serialization.

    It is slower than JSON, but allows for more data types
    to be serialized. Useful if you need to send data such as dates"""
    try:
        import yaml
        registry.register('yaml', yaml.safe_dump, yaml.safe_load,
                          content_type='application/x-yaml',
                          content_encoding='utf-8')
    except ImportError:

        def not_available(*args, **kwargs):
            """In case a client receives a yaml message, but yaml
            isn't installed."""
            raise SerializerNotInstalled(
                "No decoder installed for YAML. Install the PyYAML library")
        registry.register('yaml', None, not_available, 'application/x-yaml')


# Register instance
registry = SerializerRegistry()
encode = registry.encode
decode = registry.decode


# Register the base serialization methods.
register_json()
register_yaml()

# JSON is assumed to always be available, so is the default.
registry._set_default_serializer('json')
