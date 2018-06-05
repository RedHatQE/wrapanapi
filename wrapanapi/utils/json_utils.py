from __future__ import absolute_import
import json
import six


def json_load_byteified(file_handle):
    return _byteify(json.load(file_handle, object_hook=_byteify), ignore_dicts=True)


def json_loads_byteified(json_text):
    return _byteify(json.loads(json_text, object_hook=_byteify), ignore_dicts=True)


def _byteify(data, ignore_dicts=False):
    # if this is a unicode string, return its string representation
    if isinstance(data, six.text_type):
        return data.encode("utf-8")
    # if this is a list of values, return list of byteified values
    if isinstance(data, list):
        return [_byteify(item, ignore_dicts=True) for item in data]
    # if this is a dictionary, return dictionary of byteified keys and values
    # but only if we haven't already byteified it
    if isinstance(data, dict) and not ignore_dicts:
        return {
            _byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
            for key, value in data.iteritems()
        }
    # if it's anything else, return it in its original form
    return data
