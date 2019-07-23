"""
Tarballs and gzips all Data subfolders, then uploads them
 to NERSC HPSS using hsi.

Need to do them one folder at a time, for size considerations,
 and this code uses the maximum amount of GZIP compression available (level -9).

If we need to recover files, you can download them from NERSC and then simply
 run 'gunzip <FILE>'

NOTE: because the 'kait' folder is larger than the LocalStorage drive (even when
 compressed!) we have to move the kait subfolders one at a time.
 The Nickel folder, while very near this size limit, has not yet hit it.

-IShivvers, Dec. 2015
"""

import os
from subprocess import Popen, PIPE
from glob import glob
from datetime import datetime

#######################################################################
#### location of the Data folder to back up
BASEDIR = "/media/raid0/Data"
#### location of the scratch drive to stage tarfiles before uploading
####  MUST BE 2TB OR LARGER!
WORKINGDIR = "/media/LocalStorage/scratch"
#### location backups are saved at NERSC
NERSCDIR = "/nersc/projects/loss/backups"
#######################################################################

def backup_folder( path, ndir=NERSCDIR ):
    f = os.path.split(path)[1]
    datestamp = datetime.now().strftime('%Y%m%d')
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    outf = f +'.' + datestamp + '.tgz'
    print '\n'+'#'*20+timestamp+'#'*20+'\n'

    print 'Compressing',f,'to',outf,':'
    cmd = 'GZIP=-9 tar -cf %s/%s %s' %(WORKINGDIR,outf, path)
    print cmd
    P = Popen(cmd, stdout=PIPE,stderr=PIPE,shell=True)
    (o,e) = P.communicate()
    print o,'\n',e
    if P.returncode != 0:
        print '!!!FAILURE!!!'*10
        return

    print 'Uploading',f,'to NERSC:'
    cmd = 'cd %s; hsi "cd %s; put %s"' %(WORKINGDIR,ndir,outf)
    print cmd
    P = Popen(cmd, stdout=PIPE,stderr=PIPE,shell=True)
    (o,e) = P.communicate()
    print o,'\n',e
    if P.returncode != 0:
        print '!!!FAILURE!!!'*10
        return

    print 'Removing local copy:'
    cmd = 'rm %s/%s' %(WORKINGDIR,outf)
    print cmd
    P = Popen(cmd, stdout=PIPE,stderr=PIPE,shell=True)
    (o,e) = P.communicate()
    print o,'\n',e
    return

###################################################################
# do everything but KAIT first
folders = glob(BASEDIR+'/*')
for path in folders:
    if 'kait' in path:
        continue
    else:
        backup_folder( path )
# now do the kait subfolders
kaitfolders = glob(BASEDIR+'/kait/*')
for path in kaitfolders:
    backup_folder( path, ndir=NERSCDIR+'/kait' )

