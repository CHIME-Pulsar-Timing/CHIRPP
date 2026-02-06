#!/usr/bin/env python

"""
This script is for running on your local machine before running the v3 pipeline on Cedar.
It uses the PSRMaster database to find the current locations of all data for your pulsar.
"""

import requests
import sys

def find_tars(data):
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

    tardirs = set()
    for tar in sorted(tars):
        tarname = tar.split("/")[-1]
        n = len(tarname)
        tardir = tar[:-n].rstrip("/")
        tardirs.add(tardir)
        
    return datadirs, sorted(tars), tardirs

def print_locs(datadirs, tars, tardirs):
    print("Newer observations for this pulsar are in the following directories:\n")
    for datadir in datadirs:
        print(datadir)
    
    if len(tardirs) > 0:
        print("\nOlder data are archived here:\n")
        for tardir in tardirs:
            print(f"\n{tardir}\n")
            tars_in = [tar for tar in tars if tar[:-len(tar.split("/")[-1])].rstrip("/") == tardir]
            for tar in tars_in:
                print(f"    {tar.split("/")[-1]}\n")
    else:
        print("\nNo older data found.\n")

print("\nHave you set up an SSH Tunnel? You can do so with")
print("   ssh -L 8005:psr-head.chime:8005 $myusername@login.chimenet.ca\n")
pulsar = input(
    "Once you have done so, enter a pulsar name (e.g. B1937+21) to continue...\n"
)
print("\n")

url_base = "http://localhost:8005/v1/pulsars"

while True:
    psr_get = requests.get("{0}/{1}".format(url_base, pulsar))
    data = psr_get.json()

    if data is None:
        print(f"No observations found for PSR {pulsar}.\n")
        if len(pulsar) == 10:
            pulsar = pulsar[:-2]
            psr_get = requests.get("{0}/{1}".format(url_base, pulsar))
            data = psr_get.json()
            if data is not None:
                show = input(f"Found data for PSR {pulsar}. Show? [y/n]\n")
                if show in ["y", "Y"]:
                    datadirs, tars, tardirs = find_tars(data)
                    print_locs(datadirs, tars, tardirs)
    else:
        datadirs, tars, tardirs = find_tars(data)
        print_locs(datadirs, tars, tardirs)

    again = input("\nSearch for another pulsar? [y/n]\n")
    if again.strip() in ["y", "Y"]:
        pulsar = input(
            "\nEnter another pulsar name...\n"
        )
        print("\n")
    else:
        sys.exit(0)