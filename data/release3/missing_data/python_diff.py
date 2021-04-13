#!/usr/bin/env Python

import os
import re
import sys

nc_files = sys.argv[1]
urls_file = sys.argv[2]

urls = set()
with open(urls_file) as r:
    urls = set((line.split('/')[-1].strip() for line in r))

with open(urls_file) as r:
    url_links = set((line.strip() for line in r))

with open(nc_files) as r:
    ncfiles = set((line.strip() for line in r))

missing = urls - ncfiles

for miss in missing:
    for url in list(url_links):
        if miss in url:
            print(url)