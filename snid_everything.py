"""
Goes through all spectra in the SNDB and attempts to re-run SNID on them
 to update the SNDB entry.  Should probably be run any time we update
 SNID templates, et cetera.
"""

import MySQLdb
import MySQLdb.cursors
import credentials as creds
import SNDBLib, add2db
DB = MySQLdb.connect(host=creds.host, user=creds.user, passwd=creds.passwd, db=creds.db, cursorclass=MySQLdb.cursors.DictCursor)

root_dir = '/media/raid0/'
workingdir = './tmp'
import os
os.chdir(workingdir)
# do all work in a subfolder to make it easy to delete SNID outfile crap

# sqlfind = 'SELECT Filename,Filepath,SpecID,SNID_Type,SNID_Subtype FROM spectra;'
sqlfind = 'SELECT Filename,Filepath,SpecID,SNID_Type,SNID_Subtype FROM spectra WHERE SNID_Type = "NoMatch";'
c = DB.cursor()
c.execute(sqlfind)
while True:
    r = c.fetchoneDict()
    if r == None:
        print 'All done!'
        break
    fullpath = root_dir + r['Filepath'] + '/' + r['Filename']
    t,st = SNDBLib.getSNID( fullpath )
    if (t != r['SNID_Type']) or (st != r['SNID_Subtype']):
        # update only if needed
        sqlupdate = "UPDATE spectra SET SNID_Type = %s, SNID_Subtype = %s WHERE (SpecID = %s);"
        vals = [t,st,r['SpecID']]
        print r['Filename'],'update:'
        add2db.print_sql( sqlupdate, vals )
        c.execute( sqlupdate, vals )
        DB.commit()
    else:
        print r['Filename'],': no update.'