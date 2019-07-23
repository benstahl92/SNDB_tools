"""
Adding new files/objects/spectralruns to the database!


Any time a folder is changed, run the appropriate function:
 - import_spec_from_folder( path_to_folder )
 - import_phot_from_folder( path_to_folder )
 
-- ishivvers, May 2015
"""

import numpy as np
import MySQLdb
import MySQLdb.cursors
import credentials as creds
import re
import os
import pwd
import fnmatch
from glob import glob
import SNDBLib
from SNDBLib import *
DB = MySQLdb.connect(host=creds.host, user=creds.user, passwd=creds.passwd, db=creds.db, cursorclass=MySQLdb.cursors.DictCursor)

#################################################################
# helper functions
#################################################################

def print_sql( sql, vals=None ):
    """
    Prints an approximation of the sql string with vals inserted. Approximates cursor.execute( sql, vals ).
    Note that internal quotes on strings will not be correct and these commands probably cannot actually be
    run on MySQL - for debugging only!
    """
    print '\nRunning this SQL command to update the DB:\n'
    if vals == None:
        print sql
    else:
        s = sql %tuple( map(str, vals) )
        print s

def query_DB_object( objname=None, fitspath=None ):
    """
    Queries DB for an object.  If given fitspath (path to a fitsfile) will attempt
    to match to the coordinates pulled from the fitsfile instead of the object name.  If given both, will
    try object name first.
    """
    res = None
    if objname != None:
        sqlfind = 'SELECT ObjID FROM objects WHERE ObjName = %s;'
        c = DB.cursor()
        c.execute( sqlfind, [objname] )
        res = c.fetchone()
    if (not res) and (fitspath != None):
        # search by coordinate
        info = get_info_spec_fitsfile( fitspath )
        ra,dec = info['ra_d'],info['dec_d']
        sqlfind = 'SELECT ObjID,ObjName, (POW(RA - %s, 2) + POW(Decl - %s, 2)) AS dist FROM objects '+\
                  'HAVING dist < 10.0 ORDER BY dist LIMIT 5;' 
        vals = [ ra, dec ]
        c = DB.cursor()
        c.execute( sqlfind, vals )
        r = c.fetchmany( 5 )
        print 'Attempting to match against objects in the DB:'
        for rr in r:
            inn = raw_input( '\nUse %s? [y/n](n):\n' %rr['ObjName'] )
            if 'y' in inn.lower():
                res = rr
                break
    if res:
        return res['ObjID'] 
    
def handle_object( objname, fitspath=None ):
    """
    does the best it can to add object info based only upon a name. objname should be as you want it to be in the SNDB.
    If given a path to a fitsfile, will check for object info based upon coordinates and may interactively ask for confirmation.
    """
    # see if object already exists
    res = query_DB_object( objname, fitspath )
    if res:
        # object already exists!
        return res
    else:
        # try to parse the web to define the object
        sqlinsert = "INSERT INTO objects (ObjName, RA, Decl, Type, TypeReference, Redshift_SN, HostName, HostType, Redshift_Gal, Notes, DiscBy, DiscDate) "+\
                                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, DATE(%s));"
        vals = [objname]
        info = get_SN_info_TNS( objname )
        if not info:
            # parse rochester to get info. all characters are lowercase in ROCHESTER
            info = get_SN_info_rochester( objname )
        if not info:
            # try with a different spacing scheme
            info = get_SN_info_rochester( objname.lower().replace(' ','') )
        if (not info) and (fitspath != None):
            # try with coordinate matching pulled from fitsfile
            res  = get_info_spec_fitsfile( fitspath )
            ra,dec = res['ra_d'],res['dec_d']
            info = get_SN_info_TNS( coords=(ra,dec), interactive=5 )
            if not info:
                info = get_SN_info_rochester( coords=(ra,dec), interactive=5 )
        # parse simbad to attempt to find more info. simbad understands a variety of name formats
        info.update( get_SN_info_simbad( objname ) )
        # if neither method got us any where, raise an error
        if not info:
            raise Exception( 'Cannot find any info on this object : %s'%objname )
        for k in ['RA','Decl','Type','TypeReference','Redshift_SN','HostName','HostType','Redshift_Gal','Notes','Discoverer']:
            # if value wasn't found, value is None (NULL in MySQL)
            vals.append( info.get(k) )
        # handle the date
        date = info.get('Date')
        if date:
            date = date.split('.')[0].replace(' ','-')
        vals.append( date )
        # now go ahead and put it in
        print_sql( sqlinsert, vals )
        c = DB.cursor()
        c.execute( sqlinsert, vals )
        DB.commit()
        # now grab the ObjID from this new entry
        sqlfind = 'SELECT ObjID FROM objects WHERE ObjName = %s;'
        c.execute( sqlfind, [objname] )
        res = c.fetchone()
        c.close()
        return res['ObjID']

def handle_spectralrun( fitsfile, objname, runcode=None, interactive=True ):
    """
    fitsfile should be absolute path. objname should be as you want it in the SNDB.
    """
    # parse the input files to find the runinfo
    info = get_info_spec_fitsfile( fitsfile )
    # handle both types of date
    try:
        datestr = '%d-%d-%d' %(info['date'].year, info['date'].month, info['date'].day)
    except AttributeError:
        datestr = '%d-%d-%d' %(info['date2'].year, info['date2'].month, info['date2'].day)    
    observer = info['observer']
    sqlfind = 'SELECT RunID,Targets FROM spectralruns WHERE (UT_Date = Date(%s)) AND (Observer = %s);'
    c = DB.cursor()
    c.execute( sqlfind, [datestr, observer] )
    res = c.fetchone()
    if res:
        # associated spectral run already exists.
        if objname not in res['Targets']:
            # append this object name onto the end of the objects observed and return RunID
            sqlupdate = "UPDATE spectralruns SET Targets = %s WHERE (RunID = %s);"
            newtargets = res['Targets'] + ' | ' + objname
            print_sql( sqlupdate, [newtargets, res['RunID']] )
            c.execute( sqlupdate, [newtargets, res['RunID']] )
            DB.commit()
        return res['RunID']
    else:
        # need to parse the input files to define the spectral run
        sqlinsert = "INSERT INTO spectralruns (UT_Date, RunCode, Targets, Reducer, Observer, Instrument, Telescope, Seeing, ObservingComments, ReductionComments) "+\
                                      "VALUES (DATE(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        if runcode == None and interactive:
            runcode = raw_input('*'*20+'\nCreating a new spectralruns entry.\nEnter the run code for this run:\n' )
            if not runcode:
                runcode = None
        # handle different keywords for keck and lick files
        inst = info.get('instrument')
        if not inst:
            inst = info.get('instrument2')
        # if the reducer is close to Tom Brink, assume it was him
        if (pwd.getpwuid( os.getuid() ).pw_name == 'tgbrink') & ('rink' in info.get('reducer')):
            reducer = 'Tom Brink'
        vals = [datestr, runcode, objname, info.get('reducer'), info.get('observer'), inst, info.get('observatory'), info.get('seeing')]
        # ask user for any notes they'd like to include
        inn = raw_input('\n Enter any Observing Comments you may want to include for this run:\n')
        inn2 = raw_input('\n Enter any Reduction Comments you may want to include for this run:\n')
        if inn:
            vals.append( inn )
        else:
            vals.append( None )
        if inn2:
            vals.append( inn2 )
        else:
            vals.append( None )
        print_sql( sqlinsert, vals )
        c.execute( sqlinsert, vals )
        DB.commit()
        # now grab the RunID from this new entry
        c.execute( sqlfind, [datestr, observer] )
        res = c.fetchone()
        c.close()
        return res['RunID']
        
def handle_spectrum( flmfile, fitsfile, objname, objid=None, runid=None, just_ask=False ):
    """
    flmfile and fitsfile should be absolute paths, objname should be the name of the 
     object. can be in folder format instead of DB format.
    Can optionally include objid or runid (for objects or spectralruns tables) to override the
      code's default guesses.
    If just_ask == True, will simply query the DB to see if file already in DB.
    """
    # test to see whether the flmfile is readable
    try:
        _ = np.loadtxt(flmfile)
    except:
        raise Exception('ascii .flm file not readable; formatted wrong?')

    # parse the objname if it's not in DB format
    if re.search( '[sS][nN]\d{4}.+', objname ):
        # massage folder formats into the normal SN format in the DB
        objname = objname.replace('sn','SN ')  #together these two formats handle SN and PSN variants
        objname = objname.replace('pSN','PSN')
        if re.search( '\d{4}[a-zA-Z]$', objname ):
            # this capitalizes the letter if there's only one (i.e. 2016A); leaves them
            #  lowercase if there's 2+ (i.e. (2016ab)
            objname = objname.upper()
    # first see if this file is already in the database
    sqlfind = 'SELECT SpecID FROM spectra WHERE (Filename = %s) and (Filepath = %s);'
    fpath, fname = os.path.split( flmfile )
    # fpath is only from Data on
    fpath = fpath[ fpath.index('Data') : ]
    c = DB.cursor()
    c.execute( sqlfind, [fname, fpath] )
    res = c.fetchone()
    if res:
        # file already in database!
        return res['SpecID'], False
    elif just_ask:
        return None, False
    else:
        # need to parse the files to insert this spectrum
        sqlinsert = "INSERT INTO spectra (ObjID, RunID, Filename, Filepath, UT_Date, Airmass, Exposure, Position_Angle, Parallactic_Angle, SNR, Min, Max, Blue_Resolution, Red_Resolution, SNID_Type, SNID_Subtype, Notes) "+\
                                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );"
        # handle the object and spectralrun
        if objid == None:
            objid = handle_object( objname, fitsfile )
        if runid == None:
            runid = handle_spectralrun( fitsfile, objname )
        vals = [objid, runid, fname, fpath]
        # pull info from fitsfile 
        info = get_info_spec_fitsfile( fitsfile )
        # handle both types of date
        try:
            datestr = '%d-%d-%d' %(info['date'].year, info['date'].month, info['date'].day)
        except AttributeError:
            datestr = '%d-%d-%d' %(info['date2'].year, info['date2'].month, info['date2'].day)
        datefloat = parse_datestring( os.path.basename( flmfile ) )
        vals.append( datefloat )
        vals.extend( [info['airmass'], info['exptime'] ])
        vals.extend( [info['position_ang'], info['parallac_ang']] )
        # pull info from flmfile
        info = get_info_spec_flmfile( flmfile )
        vals.extend([ info['SNR'], info['MinWL'], info['MaxWL'] ]) # info['BlueRes'], info['RedRes'] ])
        inn = raw_input("Is this KAST data taken in the normal setup? (y/n)\n")
        if 'y' in inn:
            # For now, assume blue and red resolutions of 6 and 11 Angstroms
            vals.extend( [6, 11] )
        else:
            vals.extend( [0, 0] )
        # run snid and insert the result
        t,st = getSNID( flmfile )
        vals.extend( [t, st] )
        # ask user if there are any notes to enter
        inn = raw_input('*'*20+'\nCreating a new spectra entry.\nEnter any notes you may want to include for this spectrum.\n')
        if inn:
            vals.append( inn )
        else:
            vals.append( None )
        # actually do the insert
        print_sql( sqlinsert, vals )
        c.execute( sqlinsert, vals )
        DB.commit()
        # now grab the SpecID from this new entry
        c.execute( sqlfind, [fname, fpath] )
        res = c.fetchone()
        c.close()
        return res['SpecID'], True

def handle_lightcurve( photfile, objname ):
    """
    photfile should be absolute path to new lightcurve file. objname should be the name of the 
     object. can be in folder format instead of DB format.
    """
    # parse the objname if it's not in DB format
    if re.search( '[sS][nN]\d{4}.+', objname ):
        # massage folder formats into the normal SN format in the DB
        objname = objname.replace('sn','SN ')
        if re.search( '\d{4}[a-zA-Z]$', objname ):
            objname = objname.upper()
    fpath,fname = os.path.split( f )
    # trim the fpath relative to the Data directory
    fpath = fpath[ fpath.index('Data') : ]
    # if this file is already in the DB, update the entry instead of creating it
    sqlfind = 'SELECT PhotID FROM photometry WHERE (Filename = %s) and (Filepath = %s);'
    c = DB.cursor()
    c.execute( sqlfind, [fname, fpath] )
    res = c.fetchone()
    photid = res.get('PhotID') # this will be None if an entry doesn't yet exist
    # see if the file is public
    if 'public' in fname:
        public = 1
    else:
        public = 0
    # get the object id
    objid = handle_object( objname )
    # pull info from photfile
    firstobs, lastobs, filters, telescopes, npoints = parse_photfile( f )
    firstobs = '%d-%d-%d'%(firstobs[0],firstobs[1],firstobs[2])
    lastobs = '%d-%d-%d'%(lastobs[0],lastobs[1],lastobs[2])
    # now actually put it in
    sqlinsert = "INSERT INTO photometry (ObjID, Filename, Filepath, Filters, Telescopes, FirstObs, LastObs, NPoints, Public) "+\
                                "VALUES (%s, %s, %s, %s, %s, DATE(%s), DATE(%s), %s, %s);"
    sqlupdate = "UPDATE photometry SET ObjId = %s, Filename = %s, Filepath = %s, Filters = %s, Telescopes = %s, FirstObs = Date(%s), LastObs = Date(%s), NPoints = %s, Public = %s "+\
                "WHERE (PhotID = %s);"
    vals = [objid, fname, fpath, filters, telescopes, firstobs, lastobs, npoints, public]
    if photid != None:
        vals.append( photid )
        print_sql( sqlupdate, vals )
        c.execute( sqlupdate, vals )
    else:
        print_sql( sqlinsert, vals )
        c.execute( sqlinsert, vals )
    DB.commit()
    # now grab the PhotID from this new/updated entry
    c.execute( sqlfind, [fname, fpath] )
    res = c.fetchone()
    c.close()
    return res['PhotID']

def move_this_file( f, location ):
    # don't bother if file already exists in destination folder
    if os.path.basename(f) in map(os.path.basename, glob( location+'/*' )):
        print f,'already in',location
        return
    print '\nMoving %s to %s.' %(f,location)
    if not os.path.exists( location ):
        inn = raw_input('\nCreate new directory at\n %s\n[y/n](n):\n'%location)
        if 'y' in inn.lower():
            os.system( 'mkdir -m 770 -p %s'%location )
            os.system( 'chgrp -R flipper %s'%location )
        else:
            print '\nMust create directory to move the file.'
            return
    # now actually copy it over
    os.system( 'cp %s %s/%s'%(f, location, os.path.basename(f)) )
    os.system( 'chgrp flipper %s/%s'%(location, os.path.basename(f)) )
    os.system( 'chmod 660 %s/%s'%(location, os.path.basename(f)) )
    return

def move_files( location='./', out_root='/media/raid0/Data/spectra/', add2db=True ):
    """
    Scans the <location> folder for *.flm (and associated fits) files, and pulls the object name out of them.
    Attempts to place them in an appropriate folder under out_root.  Asks for confirmation of each move.
    """
    allflm = []
    unblotched = []
    blotched = []

    # Make list of all flm files
    # fs = glob(location+'*.flm')
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, '*.flm'):
            allflm.append(file)

    # Make lists of the blotched and unblotched flm files
    for file in allflm:
        if 'blotch' in file:
            blotched.append(file)
        else:
            unblotched.append(file)

    # Remove from unblotched list targets which have a blotched spectrum
    unblotchedonly = unblotched
    for i in blotched:
        for j in unblotched:
            if j.split('-')[0] == i.split('-')[0]:
                unblotchedonly.remove(j)

    # Combine blotched files & (for targets w/o blotched files) the unblotched files
    fs = blotched + unblotched
    fs.sort()
    # if using location='./', need to strip that from filename
    fs = map( lambda x: x.replace('./',''), fs )
    print '\nfound these files:'
    for f in fs:
        print '  ',f
    for f in fs:
        inn = raw_input('\nHandle %s? Type n to skip. [y/n](n):\n'%f)
        if 'y' not in inn.lower():
            continue
        datestr = re.search('[_-]\d{8}', os.path.basename(f)).group()
        objname = os.path.basename(f).split(datestr)[0].lower()
        move_this_file( f, os.path.join( out_root, objname ) )
        
        # find the associated files, assuming normal name convention
        #ffs = glob( os.path.join( location, objname + '*.fits' ) ) 
        ffs = glob( os.path.join( location, objname + '*' ) )
        if not ffs:
            print '\nCannot find associated fits file for',objname
        for ff in ffs:
            move_this_file( ff, os.path.join( out_root, objname, 'details' ) )
        if add2db:
            import_single_spectrum( os.path.join( out_root, objname, os.path.basename(f) ), interactive=True )
            print '\nSuccess!  Moving to next item.\n'
        else:
            print '\n',f,'moved but not entered into DB.\n'
    print 'Success! Script has completed.'

#################################################################
# main functions
#################################################################
def import_single_spectrum( path, interactive=True ):
    """
    Import a single .flm file, given the path to it.
    """
    folder, filename = os.path.split( path )
    specs = SNDBLib.yield_all_spectra( folder )
    for flm,fit in specs:
        # crude way to find flm and fitsfile, but it works.
        if filename not in flm:
            # print flm
            continue
        if not fit:
            print 'Cannot find matching fits file for',flm
            return
        objname = os.path.split(os.path.split( flm )[0])[1]
        # test to see if file already in DB
        specid,inserted = handle_spectrum( flm, fit, objname, just_ask=True )
        if specid != None:
            # already in DB
            print flm,'already in DB: specid =',specid
            return
        # if it's not in the SNDB, get info on it and import it
        objid, runid = None,None
        if interactive:
            print '\nWorking with',os.path.basename(flm)
            inn = raw_input('\no: enter objID\nr: enter runID\nn: enter Object Name\n<enter>: continue with default values\n')
            if 'o' in inn:
                in2 = raw_input('\nEnter objID:\n')
                objid = int(in2)
            if 'r' in inn:
                in2 = raw_input('\nEnter runID:\n')
                runid = int(in2)
            if 'n' in inn.lower():
                objname = raw_input('\nEnter Object Name:\n')
        print 'Object Name:',objname
        specid,inserted = handle_spectrum( flm, fit, objname, objid=objid, runid=runid )
        if inserted:
            print 'Success!  SpecID =',specid

        
def import_spec_from_folder( folder, interactive=True ):
    """
    Scan a folder for any flm files not in the DB, and insert them appropriately.
    """
    specs = SNDBLib.yield_all_spectra( folder )
    for flm,fit in specs:
        if not fit:
            print 'Cannot find matching fits file for',flm
            print '  ...skipping.'
            continue
        objname = os.path.split(os.path.split( flm )[0])[1]
        # test to see if file already in DB
        specid,inserted = handle_spectrum( flm, fit, objname, just_ask=True )
        if specid != None:
            # already in DB
            print flm,'already in DB: specid =',specid
            continue
        objid, runid = None,None
        if interactive:
            print '\nWorking with',os.path.basename(flm)
            inn = raw_input('\no: enter objID\nr: enter runID\nn: enter Object Name\n<enter>: continue\n')
            if 'o' in inn:
                in2 = raw_input('\nEnter objID:\n')
                objid = int(in2)
            if 'r' in inn:
                in2 = raw_input('\nEnter runID:\n')
                runid = int(in2)
            if 'n' in inn.lower():
                objname = raw_input('\nEnter Object Name:\n')
        specid,inserted = handle_spectrum( flm, fit, objname, objid=objid, runid=runid )
        if inserted:
            print 'Success! SpecID =',specid

def import_phot_from_folder( folder ):
    """
    Scan a folder for any phot (*.dat) files not in the DB, and insert them appropriately.
    """
    for f in glob( folder+'/*.dat' ):
        photfile = f
        objname = f.split('.')[0]
        handle_lightcurve( photfile, objname )
    
