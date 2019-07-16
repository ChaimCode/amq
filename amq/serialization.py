"""serialize: Serialize the object to JSON.
   deserialize: Deserialize JSON-encoded object to a Python object.
"""

try:
    # cjson is the fastest
    import cjson
    serialize = cjson.encode
    deserialize = cjson.decode
except ImportError:
    try:
        # Then try to find simplejson. Later versions has C speedups which
        # makes it pretty fast.
        import simplejson
        serialize = simplejson.dumps
        deserialize = simplejson.loads
    except ImportError:
        try:
            # Then try to find the python 2.6 stdlib json module.
            import json
            serialize = json.dumps
            deserialize = json.loads
        except ImportError:
            # If all of the above fails, fallback to the simplejson
            # embedded in Django.
            from django.utils import simplejson
            serialize = simplejson.dumps
            deserialize = simplejson.loads
