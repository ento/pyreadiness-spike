Script for generating the source data for [pyreadiness based on trove classifiers and wheel metadata](https://observablehq.com/d/eb336fc983fcdf52).

Initial setup:

```sh
python3 -m venv env
. env/bin/activate
pip install -r requirements.txt
```

Generate JSON files for a few Python versions:

```sh
./run.sh
```

On the first run, this makes a bunch of requests to pypi.org with no rate-limiting.

`./top-projects.json` was manually fetched from GCP's public dataset on 2022-12-16.
