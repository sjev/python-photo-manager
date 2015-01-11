__author__ = 'Jev Kuznetsov'

import os
import Image, ExifTags
import hashlib
import datetime as dt

from ConfigParser import ConfigParser
import argparse # command line argument parser
from os.path import join
import re
import sqlite3 as lite
import logging
from logging.config import fileConfig
from collections import OrderedDict

#---init 
fileConfig('logging.ini')  # set logging configuration
imageTypes = ['.jpg','.jpeg'] # supoorted image types 
USE_HASH = True # hash with md5 for file contents comparison
tagCodes = {v: k for k, v in ExifTags.TAGS.iteritems() } # get a reverse dictionary of exif tags (name-> id)

# used time formats
timeFormats = {'filename': '%Y%m%d_%H%M%S',
               'sql':'%Y-%m-%d %H:%M:%S'}
               
def humanReadaleSize(num, suffix='B'):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def str2time(t, fmt ='sql'):
    """ string to time formatter. Either provide one of the predefined time format names or custom one"""
    if fmt in timeFormats.keys():
        return dt.datetime.strptime(t ,timeFormats[fmt]) 
    else:
        return dt.datetime.strptime(t ,fmt) 

def time2str(t, fmt = 'sql' ):
    """ formats datetime type to a string according several formats """
    
    if fmt in timeFormats.keys():           
        return t.strftime(timeFormats[fmt])
    else:
        return t.strftime(fmt)
     
def newPath(path,dateRange=None):
    """ 
    create a new path based on the old one and date range of the files 
    * all directories are collapsed to max 1 subdirectory
    * folder name is prepended with year, coming from min date value    
    """
        
    parts = path.split(os.path.sep) # split with path separator
    albumName = parts[0]
    
    if not hasPrefix(albumName) and dateRange: # prepend with year
        albumName = time2str(dateRange[0],'%Y')+'_'+albumName
    
    pth = join(albumName,parts[-1]) if len(parts)>1 else albumName# go only 1 level deep.
    return pth.replace(' ','_')

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
                     'size': os.path.getsize(fName),
                     'hash': self.md5() if USE_HASH else None} #
        
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
    
    def sqlData(self, root=None):
        """ create a list ready for insertion to sql database 
            Parameters:
                root: if provided, file path will be relative to the root.
        """
        
        mapping = {'name':os.path.split(self.fName)[1],
                   'path':self.path if not root else os.path.relpath(self.path,root),
                   'dateTaken':time2str(self.info['dateTaken']) if 'dateTaken' in self.info.keys() else '',
                   'camera':self.info['camera'] if 'camera' in self.info.keys() else '',
                   'created':time2str(self.info['created']),
                   'size': self.info['size'],
                   'ext': self.info['ext'],
                   'hash': self.info['hash']}
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
    def __init__(self,root, dbFile=":memory:"):
        """
        Create class, optionally provide sqlite file name
        default database is created in memory.
        """
        
        self.log = logging.getLogger('manager')
        self.log.debug('Class created. Root:%s Database:%s' % (root,dbFile))
        
        self.root = root   
        if dbFile == ":memory:":
            self.dbFile = dbFile
        else:
            self.dbFile = join(root,dbFile)
        
            
        
        self.cols = ['name','path','ext','created','dateTaken','size','camera','hash']
        types =     ['TEXT','TEXT','TEXT','DATETIME','DATETIME','INTEGER','TEXT','TEXT']
        
        # connect to database
        self.con = lite.connect(self.dbFile)
        self.cur = self.con.cursor()
        
        
        s = ','.join([' '.join((c,t)) for c,t in zip(self.cols,types)])
        sql = 'CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY,%s)' % s
        self.cur.execute(sql)
        self.con.commit()

     
    def resetDb(self):
        """ clear database """
        self.log.info('resetting database')
        self.cur.execute('DELETE FROM files')
        self.con.commit()
        
    
    def sql(self,query):
        """ execute sql query and return result """
        self.cur.execute(query)
        data = self.cur.fetchall()
        return data
    
    def folders(self):
        """ return list of folders in database """
        return [r[0] for r in self.sql('select distinct path from files')]        
        
    
    def dateRange(self,folder):
        """ return a min-max of dateTaken of photos in folder, None if no exif data is present """
        
        sql = 'select min(dateTaken),max(dateTaken) from files where path="%s" and dateTaken <>"" ' % folder
        res = self.sql(sql)
        
        if res[0][0]:
            return str2time(res[0][0]),str2time(res[0][1])
        else:
            return None
            
    def findDuplicates(self,fName = None):
        """
        find duplicate files. Returns a (path,name,size,hash) tuples 
        If a filename is provided, the list is saved to a text file
        """
        
        sql = """SELECT  f. path, f.name, f.size, f.hash
                 FROM files f
                 INNER JOIN (
                     SELECT name,hash, count(*) as dupeCount
                     FROM files
                     GROUP by hash
                     HAVING (COUNT(hash)>1)
                     
                 ) s ON f.hash = s.hash
                 ORDER BY size DESC   """
        
        dupes = self.sql(sql)
        
        if fName:
            #-------do some sorting to make a neat printout
            hashes =[d[3] for d in dupes]
            d = OrderedDict(zip(hashes,[[] for _ in range(len(hashes))]))
            # create a dictionary with {key:[index0, index1...]}  of duplicates
            for i,hsh in enumerate(hashes):
                d[hsh].append(i)
            
            self.log.info('Saving duplicates to ' + fName)
            with open(fName,'w') as fid:
                for hsh, entries in d.iteritems():
                    first =dupes[entries[0]]
                    fid.write( first[1]+30*'-'+humanReadaleSize(first[2])+'\n')
                    for idx in entries:
                        fid.write( os.path.join(dupes[idx][0],dupes[idx][1])+'\n')

                   
        return dupes
        
    def prepareExport(self,fName='migration.ini'):
        """ 
        create an ini file that is used for exporting files.
        file is saved in the root folder
        """
                
        # create export mapping (source->dest). this is a multiline string
        
        mapping = '\n'
        for oldPath in self.folders():
            mapping+='->'.join((oldPath, newPath(oldPath,self.dateRange(oldPath))))+'\n'
        
        
        p = ConfigParser()
        p.optionxform = str 
        
        p.add_section('Export')
        p.set('Export','root',self.root)
        p.set('Export','dest',None)
        p.set('Export','mapping',mapping)
        
        if not os.path.isabs(fName):
            fName = os.path.join(self.root,fName)        
        
        self.log.info( 'Writing '+fName)
        with open(fName,'w') as fid:
            p.write(fid)
        
    
    def scan(self):
        """ scan directory tree, adding data to database """
        
        #TODO: check if path has already been added        
        
       
        for path,dirs,files in os.walk(self.root):
            print path,
            fileList = []
            for name in files:
                
                fName = os.path.join(path,name)
                if fName == self.dbFile: # exclude own database file
                    continue                 
                
                ext = os.path.splitext(name)[1].lower()
                if ext in imageTypes:
                    print '.',
                else:
                    print '?',
                    
                p = File(fName)
                fileData = [p.sqlData(root=self.root)[h] for h in self.cols]                
                
                fileList.append(fileData) # repack to a list
            
            # push to database
            sql = 'INSERT INTO files ({}) VALUES ({})'.format(','.join(self.cols),
                                                          ', '.join('?'*len(self.cols)))
        
            self.cur.executemany(sql, fileList)
            self.con.commit()
                                        
            
            
            print  ''
            
        
        
    def __repr__(self):
        return 'Photo manager\n[root] %s \n[database]%s ' % (self.root,self.dbFile)
    
    def __del__(self):
        self.con.commit()
        self.cur.close()
        self.con.close()
        
        
if __name__ == '__main__':
    
    # TODO: finish descent command-line interface 
    
    #-----------parse command line arguments    
    parser = argparse.ArgumentParser(description='Photo management toolbox')
    
    actions = ['index','copy']    
    
    parser.add_argument("root",help = 'root folder')
    parser.add_argument("action",help = 'action to perform. Possible actions: %s' % str(actions))
    parser.add_argument("-d","--database", help="database file", default="index.db")
        
    
    args = parser.parse_args()
    print args
    

    
    M = Manager(args.root,dbFile = args.database)
    print M
    
    if args.action == 'index':
        M.resetDb()
        M.scan()