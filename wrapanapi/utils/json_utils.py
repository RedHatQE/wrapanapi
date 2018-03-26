from __future__ import absolute_import

import json
import six

from ast import literal_eval

import dateparser


def json_load_byteified(file_handle):
    return _byteify(
        json.load(file_handle, object_hook=_byteify),
        ignore_dicts=True
    )


def json_loads_byteified(json_text):
    return _byteify(
        json.loads(json_text, object_hook=_byteify),
        ignore_dicts=True
    )


def _byteify(data, ignore_dicts=False):
    # if this is a unicode string, return its string representation
    if isinstance(data, six.text_type):
        return data.encode('utf-8')
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


def _try_parse_datetime(time_string):
    """Trying to parse date time from time_string. raise an error if not succeed"""
    out = dateparser.parse(time_string)
    if out:
        return out
    else:
        raise Exception('Could not parse datetime from string: {}'.format(time_string))


def _eval(text_value):
    """Trying to evaluate text_value"""
    evaluators = (
        literal_eval,
        _try_parse_datetime,
        lambda val: {'true': True, 'false': False}[val]
    )
    for eval_ in evaluators:
        try:
            return eval_(text_value)
        except Exception:
            pass
    return text_value


def eval_strings(content):
    """Recursively trying to eval any string inside json content.
        Examples:
            * 'true' -> True
            * '2016-04-14 22:09:48' -> datetime.datetime(2016, 4, 14, 22, 9, 48)
        Args:
            * content: list or tuple or any iterable array
                       representing the json content.
    """
    for i in (content if isinstance(content, dict) else range(len(content))):
        if isinstance(content[i], six.string_types):
            content[i] = _eval(content[i])
        elif hasattr(content[i], '__iter__'):
            content[i] = eval_strings(content[i])
    return content
