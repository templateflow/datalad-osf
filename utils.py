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

def osf_to_csv(osf_dict, csv):
    with open(csv, 'w') as f:
        f.write('path,url,location,sha256,path\n')
        for item in osf_dict['data']:
            name = item['attributes']['name']
            ext = ''.join(pathlib.Path(name).suffixes)
            if item['attributes']['kind'] == 'file' and ext == '.nii.gz':
                sha = item['attributes']['extra']['hashes']['sha256']
                url = item['links']['download']
                path = item['attributes']['materialized'][1:]
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
        return [''.join([url, item['attributes']['path']]) for item in superset['data']
            if item['attributes']['name'] == name]

def get_osf_recursive(urlbase, url):
    bfr = []
    superset = json_from_url(url)
    for item in superset['data']:
        if item['attributes']['kind'] == 'folder':
            bfr = bfr + get_osf_recursive(urlbase=urlbase,
                        url=''.join([urlbase, item['attributes']['path']]))
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

def update_recursive(key=None, urlbase=None, csv=None, tags=[]):
    if urlbase is None:
        urlbase = url_from_key(key)
    data = {'data': get_osf_recursive(urlbase, urlbase)}
    if csv is None:
        csv = '/tmp/recursive.csv'
    osf_to_csv(data, csv)

