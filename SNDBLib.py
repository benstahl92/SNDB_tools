###########################################################
#  A collection of python tasks for managing the 
#   flipper group's heracles computer. 
#
#   -  March, 2015 -- ishivvers, hylandm - 
#
###########################################################

import re
from glob import glob
from os import walk, path, system
import numpy as np
import matplotlib.pyplot as plt
import pyfits as pf
from difflib import get_close_matches
from cStringIO import StringIO
from subprocess import Popen,PIPE
from dateutil import parser
from urllib2 import urlopen
from bs4 import BeautifulSoup
# import julian_dates as jd

def yield_all_spectra( location='/media/raid0/Data/spectra/', include_details_flm=False, require_fits=False ):
    """
    Searches <location> (and subfolders) for all spectra.
    Finds all files named like *.flm and attempts to associate each with 
    a .fits file in the same folder (or a subfolder).
    This is an iterator, and returns paths as strings (flm, fit) one at a time.

    If include_details_flm == True, will include *.flm files found in a '/details/' subfolder.
    If require_fits == True, will only yeild *.flm files with a good match for a fitsfile too.
    
    Example: 
     > s = yield_all_spectra()
     > path_to_spec_1, path_to_fitsfile_1 = s.next()
     > path_to_spec_2, path_to_fitsfile_2 = s.next()
    """
    for root,subdirs,fnames in walk(location):
        for f in fnames:
            if re.search('.+\.flm',f):
                # we found a spectrum
                flm = root + '/'+f
                matching_fits = None
                # first look for a local matching fits file
                local_fs = map(path.basename, glob(root+'/*.fits'))
                # if there's a pretty good match, just use it
                bestmatch = get_close_matches(f, local_fs, n=1)
                if bestmatch:
                    matching_fits = '%s/%s' %(root,bestmatch[0])
                else:
                    # if there's not, then look in all subfolders one layer deep
                    if subdirs:
                        for sd in subdirs:
                            sub_fs = map(path.basename, glob(root+'/'+sd+'/*.fits'))
                            bestmatch = get_close_matches(f, sub_fs, n=1)
                            if bestmatch:
                                matching_fits = '%s/%s/%s' %(root,sd,bestmatch[0])
                                break
                # record the best matching fits file, or None if no good match found
                if not include_details_flm:
                    # do not return this pair if the flm file is in a details subfolder
                    if re.search('details', flm):
                        continue
                if require_fits and (matching_fits == None):
                    continue
                yield flm, matching_fits
 
def yield_all_images( location='/media/raid0/Data/nickel/follow/' ):
    """
    Searches <location> (and subfolders) for all images.
    Finds all files named like *.fit, *.fit.Z, *.fits, *.fits.Z, *.fts, *.fts.Z
    and yields them one at a time (this is an iterator).

    Example:
     > i = yield_all_images()
     > path_to_image_1 = i.next()
     > path_to_image_2 = i.next()
     > ...
    """
    rx = '\.((fit)|(fits)|(fts))($|\.[zZ]$)'
    for root,subdirs,fnames in walk(location):
        for f in fnames:
            if re.search(rx, f):
                yield '%s/%s' %(root,f)

def get_info_image_fitsfile( fitsfile ):
    """
    Takes in the path to an image fitsfile and attempts to pull
     from it several useful bits of information.
    
    Returns a dictionary containing the relevant header items.
    """
    try:
        hdu = pf.open( fitsfile )
    except IOError:
        # probably a zcatted file
        p = Popen(["zcat", fitsfile], stdout=PIPE)
        hdu = pf.open( StringIO(p.communicate()[0]) )
    hdu.verify('fix')
    head = hdu[0].header
    
    ks = [ ['object','object'],
           ['ra','ra'],
           ['dec','dec'],
           ['ra_d','ra'],
           ['dec_d','dec'],
           ['exptime','exposure'],
           ['exptime2','exptime'],
           ['date','date'],
           ['dateobs','date-obs'],
           ['utc','time'],
           ['date_mjd','mjd-obs'],
           ['airmass','airmass'],
           ['telescope','telescop'],
           ['instrument','instrume'],
           ['observer','observer'],
           ['filter','filters'],
           ['filter2','filtnam'] ]
    
    outdict = {}
    for outk, fitsk in ks:
        try:
            val = head[fitsk]
        except:
            val = None
        if val == None:
            pass
        elif outk in ['exptime','exptime2','date_mjd','airmass', 'utc']:
            try:
                val = float(val)
            except:
                pass
        elif outk in ['date','dateobs']:
            val = parser.parse( val ) #parse the datetime string in a reasonable way
        elif outk == 'ra_d':
            val = parse_ra( val )
        elif outk == 'dec_d':
            val = parse_dec( val )
        else:
            try:
                val = val.strip()
            except:
                pass
        outdict[outk] = val

    return outdict    

def get_info_spec_fitsfile( fitsfile ):
    """
    Takes in the path to a spectrum fitsfile and attempts to pull 
     from it several useful bits of information.
    
    Returns a dictionary of values.
    """
    hdu = pf.open( fitsfile )
    head = hdu[0].header
    
    ks = [ ['object','object'],
           ['ra','ra'],
           ['dec','dec'],
           ['ra_d','ra'],
           ['dec_d','dec'],
           ['exptime','exptime'],
           ['date','date-obs'],
           ['date2','date'],
           ['utc','utc'],
           ['date_mjd','mjd-obs'],
           ['airmass','airmass'],
           ['observatory','observat'],
           ['instrument','instrume'],
           ['instrument2','telescop'],
           ['observer','observer'],
           ['reducer','reducer'],
           ['seeing','seeing'],
           ['position_ang', 'tub'],
           ['parallac_ang', 'opt_pa'] ]
    
    outdict = {}
    for outk, fitsk in ks:
        try:
            val = head[fitsk]
        except:
            val = None
        if val == None:
            pass
        elif outk in ['exptime','date_mjd','airmass', 'position_ang', 'parallac_ang', 'seeing']:
            val = float(val)
        elif outk == 'ra_d':
            val = parse_ra( val )
        elif outk == 'dec_d':
            val = parse_dec( val )
        elif outk in ['date','date2']:
            val = parser.parse( val ) #parse the datetime string in a reasonable way
        else:
            val = val.strip()
        outdict[outk] = val
    # edit a few for historical SNDB consistency
    for outk in ['instrument', 'instrument2']:
        if outdict[outk] == None:
            continue
        if outdict[outk].lower() in ['shane', 'lick']:
            outdict[outk] = 'Kast'
            outdict['observatory'] = 'Lick 3m, Shane'
        elif 'lris' in outdict[outk].lower():
            outdict['observatory'] = 'Keck 1, 10m'
            outdict[outk] = 'LRIS'
        elif 'deimos' in outdict[outk].lower():
            outdict['observatory'] = 'Keck 2, 10m'
    return outdict

def parse_filename( f ):
    """
    Parses a *.flm file for observation date and object name.
    Returns (year, month, day), object_name
    """
    datestring = re.search('\d{8}(\.\d+)?', f).group()
    y = int(datestring[:4])
    m = int(datestring[4:6])
    d = float(datestring[6:])
    obj = f.split(datestring)[0].strip('-')
    return (y,m,d), obj

def parse_datestring( f ):
    """
    Parses a *.flm file for observation date as float.
    """
    datestring = re.search('\d{8}(\.\d+)?', f).group()
    return float(datestring)

def parse_ra( inn ):
    '''
    Parse input RA string, either decimal degrees or sexagesimal HH:MM:SS.SS (or similar variants).
    Returns decimal degrees.
    '''
    # if simple float, assume decimal degrees
    try:
        ra = float(inn)
        return ra
    except:
        # try to parse with phmsdms:
        res = parse_sexagesimal(inn)
        ra = 15.*( res['vals'][0] + res['vals'][1]/60. + res['vals'][2]/3600. )
        return ra

def parse_dec( inn ):
    '''
    Parse input Dec string, either decimal degrees or sexagesimal DD:MM:SS.SS (or similar variants).
    Returns decimal degrees.
    '''
    # if simple float, assume decimal degrees
    try:
        dec = float(inn)
        return dec
    except:
        # try to parse with phmsdms:
        res = parse_sexagesimal(inn)
        dec = res['sign']*( res['vals'][0] + res['vals'][1]/60. + res['vals'][2]/3600. )
        return dec

def parse_sexagesimal(hmsdms):
    """
    +++ Pulled from python package 'angles' +++
    Parse a string containing a sexagesimal number.
    
    This can handle several types of delimiters and will process
    reasonably valid strings. See examples.
    
    Parameters
    ----------
    hmsdms : str
        String containing a sexagesimal number.
    
    Returns
    -------
    d : dict
    
        parts : a 3 element list of floats
            The three parts of the sexagesimal number that were
            identified.
        vals : 3 element list of floats
            The numerical values of the three parts of the sexagesimal
            number.
        sign : int
            Sign of the sexagesimal number; 1 for positive and -1 for
            negative.
        units : {"degrees", "hours"}
            The units of the sexagesimal number. This is infered from
            the characters present in the string. If it a pure number
            then units is "degrees".
    """
    units = None
    sign = None
    # Floating point regex:
    # http://www.regular-expressions.info/floatingpoint.html
    #
    # pattern1: find a decimal number (int or float) and any
    # characters following it upto the next decimal number.  [^0-9\-+]*
    # => keep gathering elements until we get to a digit, a - or a
    # +. These three indicates the possible start of the next number.
    pattern1 = re.compile(r"([-+]?[0-9]*\.?[0-9]+[^0-9\-+]*)")
    # pattern2: find decimal number (int or float) in string.
    pattern2 = re.compile(r"([-+]?[0-9]*\.?[0-9]+)")
    hmsdms = hmsdms.lower()
    hdlist = pattern1.findall(hmsdms)
    parts = [None, None, None]
    
    def _fill_right_not_none():
        # Find the pos. where parts is not None. Next value must
        # be inserted to the right of this. If this is 2 then we have
        # already filled seconds part, raise exception. If this is 1
        # then fill 2. If this is 0 fill 1. If none of these then fill
        # 0.
        rp = reversed(parts)
        for i, j in enumerate(rp):
            if j is not None:
                break
        if  i == 0:
            # Seconds part already filled.
            raise ValueError("Invalid string.")
        elif i == 1:
            parts[2] = v
        elif i == 2:
            # Either parts[0] is None so fill it, or it is filled
            # and hence fill parts[1].
            if parts[0] is None:
                parts[0] = v
            else:
                parts[1] = v
    
    for valun in hdlist:
        try:
            # See if this is pure number.
            v = float(valun)
            # Sexagesimal part cannot be determined. So guess it by
            # seeing which all parts have already been identified.
            _fill_right_not_none()
        except ValueError:
            # Not a pure number. Infer sexagesimal part from the
            # suffix.
            if "hh" in valun or "h" in valun:
                m = pattern2.search(valun)
                parts[0] = float(valun[m.start():m.end()])
                units = "hours"
            if "dd" in valun or "d" in valun:
                m = pattern2.search(valun)
                parts[0] = float(valun[m.start():m.end()])
                units = "degrees"
            if "mm" in valun or "m" in valun:
                m = pattern2.search(valun)
                parts[1] = float(valun[m.start():m.end()])
            if "ss" in valun or "s" in valun:
                m = pattern2.search(valun)
                parts[2] = float(valun[m.start():m.end()])
            if "'" in valun:
                m = pattern2.search(valun)
                parts[1] = float(valun[m.start():m.end()])
            if '"' in valun:
                m = pattern2.search(valun)
                parts[2] = float(valun[m.start():m.end()])
            if ":" in valun:
                # Sexagesimal part cannot be determined. So guess it by
                # seeing which all parts have already been identified.
                v = valun.replace(":", "")
                v = float(v)
                _fill_right_not_none()
        if not units:
            units = "degrees"
    
    # Find sign. Only the first identified part can have a -ve sign.
    for i in parts:
        if i and i < 0.0:
            if sign is None:
                sign = -1
            else:
                raise ValueError("Only one number can be negative.")
    
    if sign is None:  # None of these are negative.
        sign = 1
    
    vals = [abs(i) if i is not None else 0.0 for i in parts]
    return dict(sign=sign, units=units, vals=vals, parts=parts)


def getSNID( flmfile ):
    """
    Run SNID on an ASCII spectrum,
     simply returning the best type as 
     determined by fraction and slope.
    """
    try:
        cmd = "snid plot=0 inter=0 {}".format(flmfile)
        o,e = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE).communicate()
        # if SNID found no matches at all, quit here
        if re.search('Thank you for using SNID! Goodbye.',o) == None:
            return 'NoMatch','NoMatch'

        typestring,subtypestring = o.split('Best subtype(s)')
        # find the best types first
        lines = typestring.split('\n')
        iii = lines.index(' [fraction]') +1
        ftype = lines[iii].split()[2]
        iii = lines.index(' [slope]') +1
        if 'NOTE' in lines[iii]:
            stype = ftype
        else:
            stype = lines[iii].split()[2]
        t = set([ftype,stype])
        t = ','.join(set([ftype,stype]))

        # now find best subtypes:
        lines = subtypestring.split('\n')[:10]
        iii = lines.index(' [fraction]') +1
        ftype = lines[iii].split()[3]
        iii = lines.index(' [slope]') +1
        if 'NOTE' in lines[iii]:
            stype = ftype
        else:
            stype = lines[iii].split()[3]
        st = set([ftype,stype])
        st = ','.join(set([ftype,stype]))
        if t == '':
            t = 'NoMatch'
        if st == '':
            st = 'NoMatch'
        return t, st
    except:
        return 'NoMatch','NoMatch'

def smooth( x, y, width, window='hanning' ):
    '''
    Smooth the input spectrum y (on wl x) with a <window> kernel
     of width ~ width (in x units)
    <window> options: 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'
    Returns the smoothed y array.
    '''
    if y.ndim != 1:
        raise ValueError, "smooth only accepts 1 dimension arrays."
    if x.size != y.size:
        raise ValueError, "Input x,y vectors must be of same size"
    if not window in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
        raise ValueError, "Window must be one of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'"
    avg_width = np.abs(np.mean(x[1:]-x[:-1]))
    window_len = int(round(width/avg_width))
    if y.size < window_len:
        raise ValueError, "Input vector needs to be bigger than window size."
    if window_len<3:
        return y

    s=np.r_[y[window_len-1:0:-1],y,y[-1:-window_len:-1]]

    if window == 'flat': #moving average
        w=np.ones(window_len,'d')
    else:
        w=eval('np.'+window+'(window_len)')

    y=np.convolve(w/w.sum(),s,mode='valid')
    yout = y[(window_len/2):-(window_len/2)]
    if len(yout) < len(x):
        yout = y[(window_len/2):-(window_len/2)+1]
    elif len(yout) > len(x):
        yout = y[(window_len/2):-(window_len/2)-1]
    return yout

def getSNR( flmfile, wsig=100, wnoise=500, plot=False ):
    """
    Estimates the average S/N ratio for a spectrum.
    <wsig>: the smoothing window used to estimate the signal (A)
    <wnoise>: the smoothing window used to average the noise (A)
    """
    d = np.loadtxt( flmfile )
    d = d[ ~np.isnan(d[:,1]) ] 
    sig = smooth( d[:,0], d[:,1], wsig )
    n = np.abs( d[:,1] - sig )
    noise = smooth( d[:,0], n, wnoise )
    if plot:
        plt.figure()
        plt.plot( d[:,0], d[:,1], 'k', label='data' )
        plt.plot( d[:,0], sig, 'r', label='signal' )
        plt.plot( d[:,0], n, 'gray', label='data - signal' )
        plt.plot( d[:,0], noise, 'orange', label='noise' )
        plt.legend()
        plt.xlabel('wavelength')
        plt.ylabel('flux')
        plt.title('SNR = %f'% np.mean(sig/noise) )
        plt.show()
    return np.mean( sig/noise )

def get_info_spec_flmfile( flmfile ):
    """
    Calculates a few things about the input flmfile and returns them as a dictionary.
    """
    outd = {}
    outd['SNR'] = getSNR( flmfile )
    d = np.loadtxt( flmfile )
    outd['MinWL'] = d[0,0]
    outd['MaxWL'] = d[-1,0]
    # take average resolutions of 10 pixels on either end for red/blue resolutions
    outd['BlueRes'] = np.mean(d[:,0][1:11] - d[:,0][0:10])
    outd['RedRes'] = np.mean(d[:,0][-10:] - d[:,0][-11:-1])
    return outd

def get_host_info_simbad( name ):
    """
    Queries simbad for basic info on a host galaxy.
    Returns a dictionary.
    """
    simbad_uri = "http://simbad.u-strasbg.fr/simbad/sim-id?output.format=ASCII&Ident=%s"
    result = urlopen( simbad_uri % name.replace(' ','%20') ).read()
    
    outd = {}
    # get the type of the host galaxy
    regex_type = "Morphological type:\s+[^\s]+\s"
    res_type = re.search( regex_type, result )
    try:
        outd['HostType'] = res_type.group().split(':')[1].strip()
    except AttributeError:
        pass
    # get the redshift of the host galaxy
    regex_redshift = "Redshift:\s+\d+\.\d+.+"
    res_red = re.search( regex_redshift, result )
    try:
        outd['Redshift_Gal'] = float(res_red.group().strip('Redshift: ').split(' ')[0])
        outd['Redshift_Gal_citation'] = res_red.group().split(' ')[-1]
    except AttributeError:
        pass
    return outd
        
def get_SN_info_simbad( name ):
    """
    Queries simbad for SN coords, redshift, and host galaxy.
    If redshift is not given for SN, attempts to resolve link to 
     host galaxy and report its redshift.
    Returns a dictionary.
    """
    simbad_uri = "http://simbad.u-strasbg.fr/simbad/sim-id?output.format=ASCII&Ident=%s"
    result = urlopen( simbad_uri % name.replace(' ','%20') ).read()
    outd = {}

    # try to get the coordinates
    regex_coords = "Coordinates\(FK5.+\): .+"
    res_coords = re.search( regex_coords, result )
    try:
        cs = res_coords.group().split(':')[1].strip()
        outd['RA'] = parse_ra( cs[:12].strip() )
        outd['Decl'] = parse_dec( cs[12:].strip() )
    except AttributeError:
        pass
    
    # try to get the type
    regex_type = "Spectral type: .*"
    res_type = re.search( regex_type, result )
    try:
        typrow = res_type.group().split(':')[1].split()
        typ = typrow[0]
        typ = typ.replace('SN','')
        outd['Type'] = typ
        typref = typrow[-1]
        if typref != '~':
            outd['TypeReference'] = typref
    except AttributeError:
        pass

    # try to get the redshift
    regex_redshift = "Redshift:\s+\d+\.\d+.+"
    res_red = re.search( regex_redshift, result )
    try:
        outd['Redshift_SN'] = float(res_red.group().strip('Redshift: ').split(' ')[0])
        outd['Redshift_SN_citation'] = res_red.group().split(' ')[-1]
    except AttributeError:
        pass

    # try to get the host info
    regex_host = "apparent\s+host\s+galaxy\s+.+?\{(.*?)\}"
    res_host = re.search( regex_host, result )
    try:
        host = res_host.group().split('{')[1].split('}')[0]
        outd['HostName'] = host
    except AttributeError:
        host = None
        pass
    if host != None:
        hostd = get_host_info_simbad( host )
        outd.update( hostd )

    # pull out the notes field
    try:
        outd['Notes'] = result.split('Notes')[1]
    except:
        pass
    return outd

def remove_tags( row ):
    '''returns row with HTML tags removed, for easy parsing'''
    # strip tags
    intag = False
    outstr = []
    for char in row:
        if char == '<':
            intag = True
        elif char == '>':
            intag = False
        else:
            if not intag:
                outstr.append(char)
    return ''.join(outstr)

def download_historical_rochester_info():
    """
    Parse the huge rochester SN page and produce a dictionary akin
     to that produced by download_current_rochester_info.
    """
    uri = 'http://www.rochesterastronomy.org/snimages/sndateall.html'
    page = urlopen( uri ).read()
    #soup = BeautifulSoup(page)
    soup = BeautifulSoup(page, "html.parser") # to get rid of warning triggered by above line
    table = soup.findAll("table")[1]

    C_ROCHESTER_DICT = {}
    rows = table.findChildren( recursive=False )
    for row in rows[1:]:
        vals = row.findChildren( recursive=False )
        if len(vals) == 1:
            continue
        try:
            ra = parse_ra( vals[0].getText() )
            dec = parse_dec( vals[1].getText() )
            date = vals[2].getText()
            host = vals[6].getText()
            sn_type = vals[7].getText()
            mag = float(vals[9].getText())
            name = vals[10].getText()
            altName = vals[11].getText()
            discoverer = None # not present in this table
            ref_link = None # not present in this table
            
            C_ROCHESTER_DICT[name] = [host, ra, dec, sn_type, ref_link, date, discoverer]
        except:
            # just continue on errors
            # print row
            pass
    return C_ROCHESTER_DICT
    
def download_current_rochester_info():
    """
    Parse the current rochester SN page and produce a dictionary including
     all the rows we can understand.
    This page has quite a few entries with slightly odd entries, and this script
     is my best effort to parse most of them, but there are definitely some that
     break this and are not included.  Oh well.
    """
    uri = 'http://www.rochesterastronomy.org/snimages/snactive.html'
    page = urlopen( uri ).read()
    #soup = BeautifulSoup(page)
    soup = BeautifulSoup(page, "html.parser") # to get rid of warning triggered by above line
    tables = soup.findAll("table")[1:]

    C_ROCHESTER_DICT = {}
    for t in tables:
        rows = t.findChildren( recursive=False )
        for row in rows[1:]:
            vals = row.findChildren( recursive=False )
            if len(vals) == 1:
                continue
            try:
                name = vals[0].getText()
                host = vals[1].getText()
                ra = parse_ra( vals[2].getText() )
                dec = parse_dec( vals[3].getText() )
                sn_type = vals[7].getText()
                ref_link = None # not present in this table
                date = vals[11].getText()
                discoverer = vals[12].getText()
                C_ROCHESTER_DICT[name] = [host, ra, dec, sn_type, ref_link, date, discoverer]
            except:
                # just continue on errors
                # print row
                pass
    return C_ROCHESTER_DICT

def download_rochester_info():
    print 'Downloading Rochester pages ...'
    global ROCHESTER_DICT
    C_ROCHESTER_DICT = download_current_rochester_info()
    H_ROCHESTER_DICT = download_historical_rochester_info()
    ROCHESTER_DICT = {}
    ROCHESTER_DICT.update( C_ROCHESTER_DICT )
    ROCHESTER_DICT.update( H_ROCHESTER_DICT )
    return

def get_SN_info_rochester( name=None, coords=None, interactive=0 ):
    """
    Queries dictionary built from rochester SN page for info on objects.
    If ROCHESTER_DICT has not yet been built this session, will query the page and
     parse it (takes ~10s).
    Must include either name or coords (prefers name over coords).
    If interactive >0 , will ask for confirmation if the code is not sure.
    """
    if (name == None) and (coords == None):
        raise Exception('Requires name or coords arguments!')
    global ROCHESTER_DICT
    # if we've downloaded it already this session, don't do it again!
    try:
        _ = type(ROCHESTER_DICT)
    except NameError:
        # need to build the ROCHESTER DICT
        download_rochester_info()
    
    outd = {}
    if name != None:
        # see if we have this object in the dict - all names are lowercase in the ROCHESTER_DICT
        try:
            host, ra, dec, sn_type, ref_link, date, discoverer = ROCHESTER_DICT[name.lower()]
        except KeyError:
            return {}
    elif coords != None:
        # see if we have a source that matches these coordinates
        ra = parse_ra( coords[0] )
        dec = parse_dec( coords[1] )
        keys = ROCHESTER_DICT.keys()
        diffs = [ (ROCHESTER_DICT[k][1]-ra)**2 + (ROCHESTER_DICT[k][2]-dec)**2 for k in keys ]
        i = np.argmin(diffs)
        if diffs[i] == 0.0:
            name = keys[i]
            host, ra, dec, sn_type, ref_link, date, discoverer = ROCHESTER_DICT[ name ]
        elif interactive:
            gotit = False
            dd = zip( diffs, keys )
            dd.sort()
            print 'Matching against Rochester SN page objects:'
            for attempt in range(interactive):
                name = dd[attempt][1]
                print name
                print ' host=',ROCHESTER_DICT[ name ][0]
                print ' ra=',ROCHESTER_DICT[ name ][1]
                print ' dec=',ROCHESTER_DICT[ name ][2]
                print ' date=',ROCHESTER_DICT[ name ][5]
                print ' type=',ROCHESTER_DICT[ name ][3]
                inn = raw_input( '\nIs %s the correct source? [y/n](n)\n' %name )
                if 'y' in inn.lower():
                    host, ra, dec, sn_type, ref_link, date, discoverer = ROCHESTER_DICT[ name ]
                    gotit = True
                    break
            if not gotit:
                return {}
        else:
            return {}
    
    outd['Name'] = name
    outd['HostName'] = host
    # pull any simbad info you can about the host if it has a name
    if host != 'Anon.':
        hostd = get_host_info_simbad( host )
        outd.update( hostd )
    outd['RA'] = ra
    outd['Decl'] = dec
    outd['Type'] = sn_type
    outd['TypeReference'] = ref_link
    outd['Date'] = date
    outd['Discoverer'] = discoverer
    return outd

def get_SN_info_TNS( name=None, coords=None, interactive=0 ):
    """
    Given a name and/or coordinates, finds the best match from the TNS page.
    
    If interactive is an integer (n) greater than 0, will interactively ask the user to 
     verify the first n results to choose the correct one.

    Raises an error in the case of no good matches, and returns a dictionary
     of values if a match is made.
    """
    # try a name first
    found = False
    if name != None:
        try:
            # strip it of any prefixes
            tns_name = re.search('\d{4}[a-zA-Z]+',name).group()
            uri = 'https://wis-tns.weizmann.ac.il/object/'+tns_name
            page = urlopen( uri ).read()
            #soup = BeautifulSoup(page)
            soup = BeautifulSoup(page, "html.parser") # to get rid of warning triggered by above line
            found = True
        except:
            print 'cannot find TNS source',name
    # try against coordinates only if the name query fails
    if not found and (coords != None):
        # see if we have a source that matches these coordinates
        ra = parse_ra( coords[0] )
        dec = parse_dec( coords[1] )
        uri = 'https://wis-tns.weizmann.ac.il//search?&ra=%.5f&decl=%.5f&radius=1&coords_unit=arcmin&format=csv'%(ra,dec)
        lines = urlopen(uri).readlines()
        if len(lines) == 1:
            # means no sources found
            return {}
        # parse the csv page into a dictionary
        results_dict = {}
        header = lines[0].split('","')
        for k in header:
            results_dict[k] = []
        for l in lines[1:]:
            for i,v in enumerate(l.split('","')):
                results_dict[header[i]].append(v)
        # sort by distance from queried point
        diffs = [ (parse_ra(results_dict['RA'][i])-ra)**2 + (parse_dec(results_dict['DEC'][i])-dec)**2 for i in range(len(results_dict['Name'])) ]
        i = np.argmin(diffs)
        if diffs[i] == 0.0:
            name = results_dict['Name'][i]
            tns_name = re.search('\d{4}[a-zA-Z]+',name).group()
        else:
            for i in range(min(interactive,len(results_dict['Name']))):
                print 'Matching against TNS objects by coordinates:'
                print results_dict['Name'][i]
                print ' host=',results_dict[ 'Host Name' ][i]
                print ' ra=',results_dict[ 'RA' ][i]
                print ' dec=',results_dict[ 'DEC' ][i]
                print ' date=',results_dict[ 'Discovery Date (UT)' ][i]
                print ' type=',results_dict[ 'Type' ][i]
                inn = raw_input( '\nIs %s the correct source? [y/n](n)\n' %results_dict['Name'][i] )
                if 'y' in inn.lower():
                    name = results_dict['Name'][i]
                    tns_name = re.search('\d{4}[a-zA-Z]+',name).group()
                    found = True
                    break
            if not found:
                return {}
        uri = 'https://wis-tns.weizmann.ac.il/object/'+tns_name
        page = urlopen( uri ).read()
        #soup = BeautifulSoup(page)
        soup = BeautifulSoup(page, "html.parser") # to get rid of warning triggered by above line
    elif not found:
        return {}

    # assuming we've found a match at this point
    mapdict = {'HostName':'hostname',          #keys: outd keys
               'Redshift_Gal':'host_redshift', #vals: TNS field strings
               'Redshift_SN':'redshift',
               'Discoverer':'reporter_name',
               'Type':'type',
               'Date':'discoverydate'}
    outd = {}
    for k in mapdict.keys():
        match = soup.find('div', {'class':'field-'+mapdict[k]})
        if match:
            valtxt = match.find('b').text
            if (valtxt == '---') or (valtxt == ''):
                continue
            if 'Redshift' in k:
                outd[k] = float(valtxt)
            elif k == 'Type':
                outd[k] = valtxt.replace('SN ','')
            else:
                outd[k] = valtxt
    # handle the coordinates
    radecs = soup.find('div', {'class':'field-radec'}).find('b').findAll('div')
    # for now, just take the ra,dec degrees given by alter-value if they are there
    if len(radecs) > 1:
        outd['RA'] = float(radecs[1].text.split()[0])
        outd['Decl'] = float(radecs[1].text.split()[1])
    else:
        outd['RA'] = parse_ra(radecs[0].text.split()[0])
        outd['Decl'] = parse_dec(radecs[0].text.split()[1])
    outd['Name'] = name

    # pull any simbad info you can about the host if it has a name
    if outd.get('HostName') not in [None,'Anon.']:
        hostd = get_host_info_simbad( outd.get('HostName') )
        outd.update( hostd )
    outd['TypeReference'] = 'TNS'
    return outd


def parse_photfile( f ):
    """
    Parse an ascii lightcurve file in the flipper format for entry in
     the database.
    Returns (firstobs, lastobs, filters, telescopes, npoints)
    """
    lines = [l for l in open(f,'r').readlines() if l[0]!='#']
    obsdates = [float(l.split()[0]) for l in lines]
    firstobs = min(obsdates)
    lastobs = max(obsdates)
    firstobs = jd.caldate( firstobs ) # tuple of (y,m,d,h,m,s)
    lastobs = jd.caldate( lastobs )
    filters = set([l.split()[4] for l in lines])
    filters = ','.join(filters)
    telescopes = set([l.split()[5] for l in lines])
    telescopes = ','.join(telescopes)
    npoints = len(lines)
    return (firstobs, lastobs, filters, telescopes, npoints)


