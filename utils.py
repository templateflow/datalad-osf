import urllib.request, pathlib, json
import datalad.plugin.addurls as addurls

key = 'ue5gx'

def url_from_key(key):
    return 'https://files.osf.io/v1/resources/{}/providers/osfstorage/'.format(key)

def json_from_url(url):
    with urllib.request.urlopen(url) as page:
        data = json.loads(page.read().decode())
        return data

def addurls_from_csv(csv, dataset='', filenameformat='{path}', urlformat='{url}'):
    add = addurls.Addurls()
    add(dataset=dataset,
        urlfile=csv,
        filenameformat=filenameformat,
        urlformat=urlformat,
        fast=False,
        meta=['location={location}', 'sha256={sha256}'],
        ifexists='overwrite')

def osf_to_csv(osf_dict, csv, subset=None):
    if subset is not None:
        subset_re = '/{}'.format(subset) if subset[0] != '/' else subset
        subset_re = '^{}'.format(subset_re)
        subset_re = subset_re[:-1] if subset_re[-1] == '/' else subset_re
    with open(csv, 'w') as f:
        f.write('name,url,location,sha256,path\n')
        for item in osf_dict['data']:
            name = item['attributes']['name']
            ext = ''.join(pathlib.Path(name).suffixes)
            if item['attributes']['kind'] == 'file' and ext == '.nii.gz':
                sha = item['attributes']['extra']['hashes']['sha256']
                url = item['links']['download']
                path = item['attributes']['materialized']
                path = re.sub(subset_re, '', path)[1:] if subset else path[1:]
                f.write('{},{},{},{},{}\n'.format(name, url, url, sha, path))

def get_datasets(url, name=None):
    superset = json_from_url(url)
    if name is None:
        return [(item['attributes']['name'],
                ''.join([url, item['attributes']['path']]))
                if item['attributes']['kind'] == 'folder'
                else ''.join([url, item['attributes']['path']])
                for item in superset['data']]
    else:
        return [''.join([url, item['attributes']['path']])
                for item in superset['data']
                if item['attributes']['name'] == name]

def prepare_paths(csv):
    with open(csv) as f:
        for i, line in enumerate(f):
            if i == 0:
                index = line.strip().split(',').index('path')
            else:
                path = line.split(',')[index]
                if path[-1] != '/':
                    path = '/'.join(path.split('/')[:-1])
                if path:
                    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def prepare_paths2(urlbase, url):
    superset = json_from_url(url)
    for item in superset['data']:
        if item['attributes']['kind'] == 'folder':
            path = item['attributes']['materialized'][1:]
            pathlib.Path(path).mkdir(parents=True, exist_ok=True)
            prepare_paths(urlbase=urlbase,
                          url=''.join([urlbase, item['attributes']['path']]))

def get_osf_recursive(urlbase, url, subset=None, depth=0):
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
                bfr = bfr + get_osf_recursive(
                    urlbase=urlbase,
                    url=''.join([urlbase, item['attributes']['path']]),
                    subset=subset,
                    depth=depth+1)
            else:
                bfr = bfr + [item]
    return bfr

def update_dataset(key, name, csv=None):
    url = url_from_key(key)
    data = json_from_url(get_datasets(url, name)[0])
    if csv is None:
        csv = '/tmp/{}.csv'.format(name)
    osf_to_csv(data, csv)
    addurls_from_csv(csv)

def update_recursive(key, csv=None, subset=None):
    """
    Recursively add data from an OSF project to the current datalad dataset.

    Parameters
    ----------
    key: str
        The project key on OSF. This is the string that follows `osf.io` in
        the project URL.
    csv: str or None
        Path where the CSV of files to be added will be written.
    subset: list(str) or None
        If this value is defined, then only the specified subdirectories of
        the project will be included.
    """
    urlbase = url_from_key(key)
    data = {'data': get_osf_recursive(urlbase, urlbase, subset)}
    if csv is None:
        csv = '/tmp/{}_recursive.csv'.format(key)
    osf_to_csv(data, csv, subset)
    prepare_paths(csv)
    addurls_from_csv(csv)

