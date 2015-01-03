__author__ = 'Jev Kuznetsov'

import os
import Image, ExifTags
import hashlib
import datetime as dt

import argparse # command line argument parser
from os.path import isfile, join, splitext
import re
import shutil
import sqlite3 as lite
#import pandas as pd

imageTypes = ['.jpg','.jpeg']
USE_HASH = False


# get a reverse dictionary of exif tags (name-> id)
tagCodes = {v: k for k, v in ExifTags.TAGS.iteritems() }

# our format for times YYYY-MM-DD
def formatTime(t, fmt = 'sql' ):
    """ formats datetime type to a string according several formats """
    
    formats = {'filename': '%Y%m%d_%H%M%S',
               'sql':'%Y-%m-%d %H:%M:%S'}
               
    assert fmt in formats.keys(), "fmt must be one of :" + str(formats.keys())
    
    return t.strftime(formats[fmt])

def hasPrefix(folderName):
    """ check if folder path is already prefixed with year """
    m = re.match(r"([0-9]*)[-_]", folderName)
    
    return True if m else False

   
class File(object):
    """ class for working with photos """
    def __init__(self,fName):
        
        
        self.fName = fName # full name, including path
        self.path, self.name = os.path.split(fName) # set path and name
        
        self.info = {'ext': os.path.splitext(self.fName)[1].lower(),
                     'created':dt.datetime.fromtimestamp(os.path.getctime(fName)),
                     'size': os.path.getsize(fName)} #
        
        if self.info['ext'] in imageTypes: # parse further if it is an image
            img = Image.open(fName)
            exif_data = img._getexif()
            
            # relevant tags : ['DateTimeOriginal','Model','Make']
            if exif_data:
                try:
                    self.info['dateTaken'] = dt.datetime.strptime(exif_data[tagCodes['DateTimeOriginal']] ,'%Y:%m:%d %H:%M:%S')      
                except :
                    pass
                
                try:
                    self.info['camera'] = exif_data[tagCodes['Make']].strip()+' ' + exif_data[tagCodes['Model']].strip()
                except :
                    pass


    def md5(self):
        """ calculate MD5 checksum """
       
        with open(self.fName,'rb') as fid:
            hasher = hashlib.md5()
            hasher.update(fid.read())
            hsh = hasher.hexdigest()
            
        return hsh
    
    def sqlData(self):
        """ create a list ready for insertion to sql database """
        
        mapping = {'name':os.path.split(self.fName)[1],
                   'path':self.path,
                   'dateTaken':formatTime(self.info['dateTaken']) if 'dateTaken' in self.info.keys() else '',
                   'camera':self.info['camera'] if 'camera' in self.info.keys() else '',
                   'created':formatTime(self.info['created']),
                   'size': self.info['size'],
                   'ext': self.info['ext']}
        #return None          
        return mapping         
    
    def __repr__(self):
        
        s =    'Photo: %s\n' % (str(self.fName))
        for k,v in self.info.iteritems():
            s+= '[%s]:%s\n' % (str(k),str(v))
        return  s
        
class Manager(object):
    """ 
    Class for working with files in a directory tree
    uses sqlite as backend database    
    """
    def __init__(self,dbFile=":memory:"):
        """
        Create class, optionally provide sqlite file name
        default database is created in memory.
        """
        self.cols = ['name','path','ext','created','dateTaken','size','camera']
        
        # connect to database
        self.con = lite.connect(dbFile)
        self.cur = self.con.cursor()
        
        types =     ['TEXT','TEXT','TEXT','DATETIME','DATETIME','INTEGER','TEXT']
        s = ','.join([' '.join((c,t)) for c,t in zip(self.cols,types)])
        sql = 'CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY,%s)' % s
        self.cur.execute(sql)
        self.con.commit()

     
    def resetDb(self):
        """ clears database """
        print 'resetting db'
        self.cur.execute('DELETE FROM files')
        self.con.commit()
        
    
    def sql(self,query):
        self.cur.execute(query)
        data = self.cur.fetchall()
        return data
        
    def scan(self,root):
        """ scan directory tree, adding data to database """
        
        #TODO: check if path has already been added        
        fileList = []
       
        for path,dirs,files in os.walk(root):
            print path,
            
            for name in files:
                
                fName = os.path.join(path,name)
                ext = os.path.splitext(name)[1].lower()
                if ext in imageTypes:
                    print '.',
                else:
                    print '?',
                    
                p = File(fName)
                    
                fileList.append([p.sqlData()[h] for h in self.cols]) # repack to a list
                                    
            print  ''
            
        # push to database
        sql = 'INSERT INTO files ({}) VALUES ({})'.format(','.join(self.cols),
                                                          ', '.join('?'*len(self.cols)))
        
        self.cur.executemany(sql, fileList)
        self.con.commit()
        
    def __del__(self):
        self.con.commit()
        self.cur.close()
        self.con.close()
        
        
if __name__ == '__main__':
    
    #-----------parse command line arguments    
    parser = argparse.ArgumentParser(description='Photo management toolbox')
    
    actions = ['update','copy']    
    
    parser.add_argument("source",help = 'source folder')
    parser.add_argument("action",help = 'action to perform. Possible actions: %s' % str(actions))
    parser.add_argument("-d","--database", help="database file", default="photos.db")
        
    
    args = parser.parse_args()
    print args
    
    print 'Source: ', args.source
    print 'Database: ', args.database
    
    M = Manager(args.database)
    
    if args.action == 'update':
        M.resetDb()
        M.scan(args.source)