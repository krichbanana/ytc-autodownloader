#!/usr/bin/env python3
import os

import scraper_oo
from scraper_oo import (
    AutoScraper,
    alt_main,
)


def main():
    context = AutoScraper()
    scraper_oo.is_true_main = False
    scraper_oo.main_autoscraper = context
    scraper_oo.altpid = os.getpid()
    alt_main(context=context)


if __name__ == '__main__':
    if os.path.exists('oo') and not os.path.exists('../oo'):
        # avoid oo-in-oo, for... reasons.
        print('Auto-chdir to \'oo\'')
        os.chdir('oo')
        print("CWD:", os.getcwd())
    main()
