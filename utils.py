import urllib.request, pathlib, json
import datalad.plugin.addurls as addurls

url = 'https://files.osf.io/v1/resources/ue5gx/providers/osfstorage/'

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
        fast=True,
        meta=['location={location}', 'sha256={sha256}'],
        ifexists='overwrite')

def osf_to_csv(osf_dict, csv):
    with open(csv, 'w') as f:
        f.write('path,url,location,sha256\n')
        for item in osf_dict['data']:
            name = item['attributes']['name']
            ext = ''.join(pathlib.Path(name).suffixes)
            if item['attributes']['kind'] == 'file' and ext == '.nii.gz':
                sha = item['attributes']['extra']['hashes']['sha256']
                url = item['links']['download']
                f.write('{},{},{},{}\n'.format(name, url, url, sha))

def get_datasets(url, name=None):
    superset = json_from_url(url)
    if name is None:
        return [''.join([url, item['attributes']['path']]) for item in superset['data']]
    else:
        return [''.join([url, item['attributes']['path']]) for item in superset['data']
            if item['attributes']['name'] == name]

def update_dataset(url, name, csv=None):
    data = json_from_url(get_datasets(url, name)[0])
    if csv is None:
        csv = '/tmp/{}.csv'.format(name)
    osf_to_csv(data, csv)
    addurls_from_csv(csv)
