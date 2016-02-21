#!/usr/bin/env python
"""
   Downloads all available Google Vertical Trend Spreadsheets for motherboard
"""
import ast
import argparse
import re
from multiprocessing import Pool
from functools import partial
import requests
import gspread
from oauth2client.client import AccessTokenCredentials
import gvt_config_mobo as config

def authenticate_google_docs():
    """ Authenticate to the google docs api and return a gspread object
        Further reference to how to generate various codes
        http://www.indjango.com/access-google-sheets-in-python-using-gspread/
    """

    data = {
        'refresh_token' : config.refresh_token,
        'client_id' : config.client_id,
        'client_secret' : config.client_secret,
        'grant_type' : 'refresh_token',
    }

    req = requests.post('https://accounts.google.com/o/oauth2/token', data=data)

    gdoc = gspread.authorize(
        AccessTokenCredentials(ast.literal_eval(req.text)['access_token'], 'Test'))

    return gdoc


def get_file_list(gdoc):
    """
        Authenticates to google docs and gets a list of all spreadsheets accessible
        to the login creds. Returns a list containing spreadsheets where the spreadsheet
        name contains 'VT2''PASCAT' e.g. "VT2 - PASCAT_12840 Pre-Packaged Deli Meats"

        Parameters:
            gc - instance of gspread auth object
    """
    gdoc = authenticate_google_docs()
    sheets = gdoc.openall()

    # only accept file names containing 'VT2' | 'PASCAT' and 5 digit code
    pat = re.compile('(?=VT2)(?=.*PASCAT)(?=.*\\d{5})')

    file_list = [s for s in sheets for m in [pat.match(s.title)] if m]

    return file_list


def export_file(file, out_path):
    """ Opens the spreadsheet using the file handle passed to it
        Exports the first worksheet within the spreadsheet to
        a tsv.
    """
    fname = file.title

    # NB - removed .read(), all of a sudden this method doesn't work.
    # Must've been a change somewhere, but don't know where...
    data = file.worksheets()[0].export(format='tsv')#.read()
    with open(out_path + fname + '.txt', 'wb') as outfile:
        outfile.write(data)
        print('Download complete:', fname)


def make_parser():
    """ Builds command line parser
    """
    parser = argparse.ArgumentParser(description=main.__doc__)

    parser.add_argument("-op", "--outpath", help="path of output files", required=False)
    parser.add_argument("-v", "--verbose", help="display logging messages")

    return parser

def main():
    """
        main function
    """

    # get input arguments
    args = make_parser().parse_args()
    out_path = args.outpath

    # connect to Google and get doc list
    gdoc = authenticate_google_docs()
    flist = get_file_list(gdoc)

    if len(flist) == 0:
        print("No files to download")
    else:
        # create partial function with out_path value filled in
        partial_export_file = partial(export_file, out_path=out_path)

        # create and process mp pool
        # another comment
        #more comments!!
        pool = Pool(10)
        pool.map(partial_export_file, flist)
        pool.close()
        pool.join()

if __name__ == '__main__':
    main()
    