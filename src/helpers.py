"""Helper functions"""

import logging

logging_config = dict(
    version=1,
    formatters={
        'f': {'format':
                  '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
    },
    handlers={
        'h': {'class': 'logging.StreamHandler',
              'formatter': 'f',
              'level': logging.INFO}
    },
    root={
        'handlers': ['h'],
        'level': logging.INFO,
    },
)


def find_unique(s):
    uniques = s.dropna().unique()
    if len(uniques) > 1:
        return ', '.join([item for item in uniques])
    else:
        return uniques
