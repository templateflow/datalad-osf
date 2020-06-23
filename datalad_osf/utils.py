#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""Utility functions for a recursive datalad downloader for OSF.

Example
-------
key = 'ue5gx' # templateflow dataset
subset = 'tpl-NKI'
update_recursive(key, subset, limit_to_ext='.nii.gz')

"""
import urllib.request
import pathlib
import json
import re
from csv import DictReader

import datalad.plugin.addurls as addurls


def url_from_key(key):
    """Given an OSF project key, retrieve the URL of the project metadata."""
    return ('https://files.osf.io/v1/resources/{}'
            '/providers/osfstorage/'.format(key))


def json_from_url(url):
    """Download JSON metadata at target URL into python dictionary."""
    with urllib.request.urlopen(url) as page:
        data = json.loads(page.read().decode())
        return data


def addurls_from_csv(csv, filenameformat='{path}', urlformat='{url}'):
    """Add URLs to datalad repository.

    Use datalad to add data from the URLs specified in a provided CSV file
    into the paths specified in the CSV file, generating any required
    directory structure in the process.

    Parameters
    ----------
    csv : str
        Path to the CSV where the metadata should be stored.
    filenameformat : str
        String format to use to extract filesystem paths from the CSV (based
        on headers in the OSF dictionary). For instance, `{path}` indicates
        that each file's path should be generated from the `path` column of
        the CSV.
    urlformat : str
        String format to use to extract URLs from the CSV (based on headers
        in the OSF dictionary). For instance, `{url}` indicates that each
        file's URL should be generated from the `url` column of the CSV.

    """
    prepare_paths(csv, filenameformat=filenameformat)
    add = addurls.Addurls()
    add(dataset='',
        urlfile=csv,
        filenameformat=filenameformat,
        urlformat=urlformat,
        fast=False,
        meta=['location={location}', 'sha256={sha256}'],
        ifexists='overwrite')


def osf_to_csv(osf_dict, csv, subset=None, limit_to_ext='.nii.gz'):
    """Construct a CSV file from OSF metadata.

    Parameters
    ----------
    osf_dict : dict
        Dictionary of file metadata pulled from OSF, as by `json_from_url`.
    csv : str
        Path to the CSV where the metadata should be stored.
    subset : str | None
        If this value is defined, then only the specified subdirectories of
        the project will be included.
    limit_to_ext : str | None
        If this value is defined, only files with names that end with
        `limit_to_ext` are written to the `csv`.  Defaults to '.nii.gz'.

    """
    if subset is not None:
        subset_re = '/{}'.format(subset) if subset[0] != '/' else subset
        subset_re = '^{}'.format(subset_re)
        subset_re = subset_re[:-1] if subset_re[-1] == '/' else subset_re
    with open(csv, 'w') as f:
        f.write('name,url,location,sha256,path\n')
        for item in osf_dict['data']:
            name = item['attributes']['name']
            if limit_to_ext is not None:
                if not name.endswith(limit_to_ext):
                    continue
            if item['attributes']['kind'] == 'file':
                sha = item['attributes']['extra']['hashes']['sha256']
                url = item['links']['download']
                path = item['attributes']['materialized']
                path = re.sub(subset_re, '', path)[1:] if subset else path[1:]
                f.write('{},{},{},{},{}\n'.format(name, url, url, sha, path))


def prepare_paths(csv, filenameformat='{path}'):
    """Mirror OSF directory structure to generate datalad directory structure.

    Parameters
    ----------
    csv : str
        CSV containing metadata and paths for the datalad dataset, with
        paths defined relative to the dataset root directory.
    filenameformat : str
        String format to use to extract filesystem paths from the CSV (based
        on headers in the OSF dictionary). For instance, `{path}` indicates
        that each file's path should be generated from the `path` column of
        the CSV.

    """
    with open(csv) as file:
        reader = DictReader(file)
        for row in reader:
            path = filenameformat.format(**row)
            path = '/'.join(path.split('/')[:-1]) if path[-1] != '/' else path
            if path:
                pathlib.Path(path).mkdir(parents=True, exist_ok=True)


def _get_osf_recursive(urlbase, url, subset=None, depth=0):
    """Retrieve metadata for all files from OSF project or its `subset`."""
    bfr = []
    superset = json_from_url(url)
    if subset is not None:
        subset_re = subset[1:] if subset[0] == '/' else subset
        subset_re = '/'.join(subset_re.split('/')[0:depth+1])
        subset_re = '^/{}'.format(subset_re)
    else:
        subset_re = None
    for item in superset['data']:
        if subset is None or re.search(subset_re,
                                       item['attributes']['materialized']):
            if item['attributes']['kind'] == 'folder':
                bfr = bfr + _get_osf_recursive(
                    urlbase=urlbase,
                    url=''.join([urlbase, item['attributes']['path']]),
                    subset=subset,
                    depth=depth+1)
            else:
                bfr = bfr + [item]
    return bfr


def get_osf_recursive(url, subset=None):
    """Retrieve metadata for all files from OSF project or its `subset`.

    Parameters
    ----------
    url : str
        The OSF project's metadata URL.
    subset : str | None
        If this value is defined, then only the specified subdirectories of
        the project will be included.

    """
    return _get_osf_recursive(url, url, subset, depth=0)


def update_recursive(key, csv=None, subset=None, limit_to_ext='.nii.gz'):
    """Recursively add data from OSF project to the current datalad dataset.

    Parameters
    ----------
    key : str
        The project key on OSF. This is the string that follows `osf.io` in
        the project URL.
    csv : str | None
        Path where the CSV of files to be added will be written.
    subset : list of str | None
        If this value is defined, then only the specified subdirectories of
        the project will be included.
    limit_to_ext : str | None
        If this value is defined, only files with names that end with
        `limit_to_ext` are written to the `csv`. Defaults to '.nii.gz'.

    """
    url = url_from_key(key)
    data = {'data': get_osf_recursive(url, subset)}
    if csv is None:
        csv = '/tmp/{}_recursive.csv'.format(key)
    osf_to_csv(data, csv, subset, limit_to_ext)
    addurls_from_csv(csv)
