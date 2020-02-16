FROM daskdev/dask:2.3.0

# Install some necessary tools.
RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
&& rm -rf /var/lib/apt/lists/*

# Add local impresso_pycommons
ADD . .

RUN pip install --upgrade pip
RUN pip install \
    git+https://github.com/dkpro/dkpro-pycas.git \
    git+https://github.com/impresso/dask_k8.git \
    boto \
    boto3 \
    bs4 \
    docopt \
    "kubernetes>=9.0.0,<10" \
    "urllib3>1.21.1<1.25" \
    "opencv-python>=3.4,<=4" \
    smart_open \
    jsonlines \
    s3fs \
    jupyter

RUN python setup.py install
