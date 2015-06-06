# -*- coding: utf-8 -*-
"""
copy missing files
"""

import os
from shutil import copyfile

source_root = '/media/jev/Data/Photo'
dest_root = '/media/jev/USB_160/Photo_missing'

fileList = 'output/missing.txt'

ignore_ext = ['.ini','.db']

with open(fileList,'r') as fid:
    lines = fid.readlines()
    
    
for idx, line in enumerate(lines):
    print '[%i/%i] ' % (idx, len(lines)),    
    
    if line[0] == '#': 
        continue
    
    else: #process
        l = line.strip()
        p,n = os.path.split(l)
        src = os.path.join(source_root,p,n)
        
        _,ext = os.path.splitext(src)
        if ext in ignore_ext:
            print 'Skipping ', src
            continue
        
        
        dest_p = os.path.join(dest_root,p)
        if not os.path.exists(dest_p):
            print 'creating directory ', dest_p 
            os.makedirs(dest_p)
        
        dest = os.path.join(dest_root,p,n)
        
        print src ,' -> ', dest,
        if not os.path.exists(dest):
            copyfile(src,dest)
            print 'copied'
        else:
            print 'present'
        
