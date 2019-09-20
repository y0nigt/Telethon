# coding: utf-8

from __future__ import annotations
import logging
import sys
import traceback
from typing import Dict, Union, List

import phonenumbers
from phonenumbers import carrier
from phonenumbers import geocoder

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def parse_full(phone: str, context: str = None, lang: str = 'EN') -> Union[Dict, None]:
    try:
        if not phone:
            return None

        if context:
            parsed = phonenumbers.parse(phone, context.upper())
        else:
            parsed = phonenumbers.parse(phone)

        carrier_res = carrier.name_for_number(parsed, lang.upper())
        local_res = geocoder.description_for_number(parsed, lang.upper())

        national_fmt = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.NATIONAL)  # '020 8366 1177'

        full_pretty_fmt = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL)  # '+44 20 8366 1177'

        full_compact_fmt = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)  # '+442083661177'

        result = {
            'carrier':      carrier_res,
            'local_res':    local_res,
            'country_code': parsed.country_code,
            'national':     parsed.national_number,
            'ext':          parsed.extension,
            'formatted':    {
                'national':     national_fmt,
                'full_pretty':  full_pretty_fmt,
                'full_compact': full_compact_fmt,
            },
        }

        logger.info(f'Parse phone={phone} with context={context} and lang={lang.upper()} produced result={result}.')

        return result

    except:
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)

        logger.error(f'Failed to parse phone={phone} with context={context} and lang={lang}.', exc_info, exc_info=True)
        return None


def find_in_text(text: str, context: str = None, lang: str = 'EN') -> Union[List[Dict], None]:
    try:
        if not text:
            return None

        results = phonenumbers.PhoneNumberMatcher(text, context.upper())

        if results:
            logger.info(f'Parse text={text} with context={context} and lang={lang.upper()} produced results={results}.')
            return [parse_full(result.raw_string, context, lang.upper()) for result in results if result and hasattr(result, 'raw_string')]
        else:
            logger.info(f'Parse text={text} with context={context} and lang={lang.upper()} produced no results.')
            return None
    except:
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)

        logger.error(f'Failed to parse text={text} with context={context} and lang={lang.upper()}.', exc_info, exc_info=True)
        return None
