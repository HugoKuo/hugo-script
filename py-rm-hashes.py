#! /usr/bin/python

import sys
import subprocess
import re

hfile = "handoff-10.txt"

with open(hfile) as f:
    label_parts = f.readlines()

for line in label_parts:
    x = line.split(' ')
    job = {x[0]:x[1], x[2]:x[3].strip('\n')}
    ### Remove hashes pickles ###
    for i in job['-p'].split(','):
        print "Delete pickles for" + " " + job['-d'] + " " + i
        target_path_invalid = "/srv/node/" + job['-d'] + "/objects-10/" + i + "/hashes.invalid"
        target_path_pkl = "/srv/node/" + job['-d'] + "/objects-10/" + i + "/hashes.pkl"
        try:
            rm_output_invalid = subprocess.check_output(["rm", "-v", target_path_invalid])
            print rm_output_invalid
        except:
            pass
        try:
            rm_output_pkl = subprocess.check_output(["rm", "-v", target_path_pkl])
            print rm_output_pkl
        except:
            pass
