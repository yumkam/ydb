#!/usr/bin/env python3
import hashlib
for Id in range(10000000):
    h = hashlib.sha256(bytes(str(Id), 'utf-8')).hexdigest()
    print(Id, Id % 31, h, sep="\t")
