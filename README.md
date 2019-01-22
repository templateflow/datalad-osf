# datalad-osf
Utility scripts to facilitate use of Datalad with OSF

## Usage

To download an OSF project into the current datalad dataset, use the provided `update_recursive` function. `update_recursive` accepts two parameters: the OSF project key (obtainable from the project URL) and an optional `subset` parameter, which allows downloading a subset of the OSF project.

Before beginning, navigate to the datalad dataset directory on your file system (e.g., `cd dataset-001/`) and ensure that it is a valid datalad dataset. (If not, you can initialise it quickly by calling `datalad create`.)

The project key is just the set of characters that follow `osf.io/` in your project's URL. For instance, the project key for [`templateflow`](https://osf.io/ue5gx/) is `ue5gx`.

The `subset` parameter can be used if you want to include, for instance, only data from a single template in the datalad dataset. Define subset as `tpl-NKI`, and only files in the `tpl-NKI` subdirectory will be synced to the datalad dataset. `subset` can be a path to any valid directory in your OSF project, defined relative to the top level of the project.
```
key='ue5gx'
subset = 'tpl-NKI'

import datalad_osf
datalad_osf.update_recursive(key, subset)
```
`update_recursive` uses the OSF project metadata to determine the directory structure required to locally replicate (any subset of) the OSF dataset and then adds the files or references synced from OSF to the datalad dataset.
