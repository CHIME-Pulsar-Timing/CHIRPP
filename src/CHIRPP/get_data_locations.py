#!/usr/bin/env python

"""
This script is for running on your local machine before running the v3 pipeline on Cedar.
It uses the PSRMaster database to find the current locations of all data for your pulsar.
"""

import requests

print("\nHave you set up an SSH Tunnel? You can do so with")
print("   ssh -L 8005:psr-head.chime:8005 $myusername@login.chimenet.ca\n")
pulsar = input(
    "Once you have done so, enter a pulsar name (e.g. B1937+21) to continue...\n"
)
print("\n")

url_base = "http://localhost:8005/v1/pulsars"

psr_get = requests.get("{0}/{1}".format(url_base, pulsar))
data = psr_get.json()

paths = [x["current_location"] for x in data["observation_details"]]

datadirs = set()
tars = set()

for path in paths:
    if path:
        if path.endswith(".tar"):
            tars.add(path)
        elif path.endswith(".ar"):
            datafile = path.split("/")[-1]
            n = len(datafile)
            loc = path[:-n].rstrip("/")
            datadirs.add(loc)
        else:
            loc = path.rstrip("/")
            datadirs.add(loc)

print("Newer observations for this pulsar are in the following directories:\n")
for datadir in datadirs:
    print(datadir)

print("\nI also found the following .tar archives of older data:\n")

tardirs = set()
for tar in sorted(tars):
    tarname = tar.split("/")[-1]
    n = len(tarname)
    tardir = tar[:-n].rstrip("/")
    tardirs.add(tardir)
    print(tar)

print("\nThey can be copied with:\n")

for tardir in tardirs:
    print(f"   cp {tardir}/{pulsar}*.tar .\n")
