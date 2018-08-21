import fauxfactory


def random_name(prefix='integration-tests', length=5):
    return '{}-{}'.format(prefix, fauxfactory.gen_alphanumeric(length=length).lower())
