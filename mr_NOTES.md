# Notes

## Rebuilding on the cluster

```bash
# launch the dask scheduler
screen -dmS  dask_scheduler dask-scheduler

# launch a buck of dask workers
screen -dmS dask_workers dask-worker <HOST>:8786 --nprocs 36 --nthreads 1 --memory-limit 10G

# launch the actual process
screen -dmSL impresso-rebuild python impresso_commons/text/rebuilder.py rebuild_articles --input-bucket=original-canonical-data --log-file=/scratch/matteo/impresso-rebuilt/logs/rebuild-20180925.log --output-dir=/scratch/matteo/impresso-rebuilt/data/ --filter-config=impresso_commons/data/config/GDL.json  --scheduler=<HOST>:8786 --format='solr' --output-bucket='canonical-rebuilt-lux'

screen -dmSL impresso-rebuild python impresso_commons/text/rebuilder.py rebuild_articles --input-bucket=original-canonical-data --log-file=/scratch/matteo/impresso-rebuilt/logs/rebuild-20181119.log --output-dir=/scratch/matteo/impresso-rebuilt/data/ --filter-config=impresso_commons/data/config/JDG.json  --scheduler=<HOST>:8786 --format='solr' --output-bucket='canonical-rebuilt'
```


## Testing the rebuilding functions

while refactoring the rebuld function

```python
%load_ext autoreload
%autoreload 2

from impresso_commons.text.helpers import *
from impresso_commons.path.path_fs import IssueDir
from impresso_commons.path.path_s3 import impresso_iter_bucket
from impresso_commons.utils.s3 import get_bucket
from impresso_commons.text.rebuilder import *

input_bucket_name = "original-canonical-data"
issues = impresso_iter_bucket(
        input_bucket_name,
        #prefix="GDL/1950/01",
        prefix="NZZ/1780/01",
        item_type="issue"
    )
len(issues)

issue = IssueDir(**issues[18])
b = 'original-canonical-data'
x = read_issue(issue, b)
y = read_issue_pages(x[0], x[1], bucket=b)
z = rejoin_articles(y[0], y[1])
articles = [pages_to_article(a, p) for a, p in z]
art = articles[4]
q = rebuild_for_solr(art)


def check_containment(paragraph, regions):
    r = [region for region in regions if paragraph in region]
    return True if len(r) > 0 else False

```

## writing to HDFS

`hdfs://iccluster050.iccluster.epfl.ch:8020/user/romanell/`
