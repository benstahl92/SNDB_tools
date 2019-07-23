"""
Interactively search for objects in the SNDB that have been given updated names.

-ishivvers, Nov. 2015

"""

import MySQLdb
import MySQLdb.cursors
import credentials as creds
import SNDBLib
import re, glob

DB = MySQLdb.connect(host=creds.host, user=creds.user, passwd=creds.passwd, db=creds.db, cursorclass=MySQLdb.cursors.DictCursor)

def print_sql( sql, vals=None ):
    """
    Prints an approximation of the sql string with vals inserted. Approximates cursor.execute( sql, vals ).
    Note that internal quotes on strings will not be correct and these commands probably cannot actually be
    run on MySQL - for debuggin only!
    """
    print '\nSQL command:\n'
    if vals == None:
        print sql
    else:
        s = sql %tuple( map(str, vals) )
        print s

def get_associated_spectra( objid ):
    sqlfind = 'SELECT * from spectra where ObjID = %s'
    c = DB.cursor()
    c.execute( sqlfind, [objid] )
    return c.fetchall()

def get_coords_from_file( objid ):
    # try to pull the coordinates from the spectrum file
    coords = None
    res = get_associated_spectra( objid )
    for r in res:
        path = '/media/raid0/'+r['Filepath']+'/details/*.fits'
        fs = glob.glob( path )
        if len(fs) == 0:
            continue
        for f in fs:
            info = SNDBLib.get_info_spec_fitsfile( f )
            if info['ra_d'] and info['dec_d']:
                return info['ra_d'],info['dec_d'],info['date']

def query_DB_objects( regexp='[Pp][Ss][Nn]' ):
    """
    Queries DB for all objects that have a ObjName field matching the RegExp, and
     which do not have an AltObjName entry.
    """
    sqlfind = 'SELECT * FROM objects WHERE (ObjName REGEXP %s) AND (AltObjName IS NULL);'
    c = DB.cursor()
    c.execute( sqlfind, [regexp] )
    res = c.fetchall()
    # try to get all the best Rochester matches
    for r in res:
        print '\n',r['ObjName'],':::',r['RA'],':',r['Decl'],':',r['DiscDate']
        updateCoords = False
        if (r['RA']==0.0) and (r['Decl']==0.0):
            try:
                r['RA'],r['Decl'],obsdate = get_coords_from_file( r['ObjID'] )
            except:
                print 'Failed to pull out coordinate info'
                continue
            print 'updated coordinate info.'
            print r['ObjName'],':::',r['RA'],':',r['Decl'],':',obsdate
            updateCoords = True
        info = SNDBLib.get_SN_info_rochester( coords=(r['RA'],r['Decl']), interactive=5 )
        if info:
            inn = raw_input('Use %s to update SNDB entry?\n'%info['Name'] +\
                            ' a: just altname\n m: just main name\n y: everything else too\n n: no.\n')
            if 'n' not in inn.lower():
                objname = info['Name']
                if re.search( '[sS][nN]\d{4}.+', objname ):
                    # massage name formats into the normal SN format in the DB
                    objname = objname.replace('sn','SN ')  #together these two formats handle SN and PSN variants
                    objname = objname.replace('pSN','PSN')
                    if re.search( '\d{4}[a-zA-Z]$', objname ):
                        objname = objname.upper()
                sqlupdate = "UPDATE objects SET "
                sqlvars = []
                if 'a' in inn.lower():
                    sqlupdate += 'AltObjName = %s' 
                    sqlvars.append( objname )
                elif 'm' in inn.lower():
                    sqlupdate += 'ObjName = %s, AltObjName = %s' 
                    sqlvars.extend( [objname, r['ObjName']] )
                else:
                    raise Exception('Need to include m or a.')
                if updateCoords:
                    sqlupdate += ", RA = %s, Decl = %s"
                    sqlvars.extend( [r['RA'], r['Decl']] )
                if 'y' in inn.lower():
                    sqlupdate += ", Type = %s, DiscDate = %s, Discoverer = %s, HostName = %s"
                    sqlvars.extend( [info['Type'],info['Date'],info['Discoverer'],info['HostName']] )
                sqlupdate += " WHERE ObjID = %s;"
                sqlvars.append( r['ObjID'] )
                print_sql( sqlupdate, sqlvars )
                c.execute( sqlupdate, sqlvars )
                DB.commit()
                print 'Updated:',r['ObjName'],'=',info['Name']

if __name__ == '__main__':
    query_DB_objects()
