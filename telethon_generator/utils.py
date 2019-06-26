import re


def snake_to_camel_case(name):
    # Courtesy of http://stackoverflow.com/a/31531797/4759433
    result = re.sub(r'_([a-z])', lambda m: m.group(1).upper(), name)
    return result[:1].upper() + result[1:].replace('_', '')
