# Set base image
FROM daskdev/dask:latest-py3.11

# Set environment variables for user
ENV GROUP_NAME=DHLAB-unit
ENV GROUP_ID=11703

ARG USER_NAME
ARG USER_ID

# Install build tools and libraries
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        pkg-config \
        cmake \
        software-properties-common 

RUN DEBIAN_FRONTEND=noninteractive apt-get install -y \
    apt-utils \
    git  \
    curl  \
    vim  \
    unzip  \
    wget  \
    tmux  \
    screen  \
    wget \
    sudo \
    openssh-client

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create a group and user
RUN groupadd -g $GROUP_ID $GROUP_NAME
RUN useradd -ms /bin/bash -u $USER_ID -g $GROUP_ID $USER_NAME

# Add new user to sudoers
RUN echo "${USER_NAME} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Add Conda
ENV CONDA_PREFIX=/home/${USER_NAME}/.conda
ENV CONDA=/home/${USER_NAME}/.conda/condabin/conda

RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && \
    bash miniconda.sh -b -p ${CONDA_PREFIX} && \
    rm miniconda.sh && \
    ${CONDA} config --set auto_activate_base false && \
    ${CONDA} init bash && \
    ${CONDA} create --name rebuilt python=3.11

ENV PATH="/home/${USER_NAME}/.conda/envs/rebuilt/bin:$PATH"

RUN /home/${USER_NAME}/.conda/condabin/conda create -n rebuilt python=3.11 pip

RUN /home/${USER_NAME}/.conda/condabin/conda run -n rebuilt pip install --upgrade pip setuptools
RUN /home/${USER_NAME}/.conda/condabin/conda run -n rebuilt pip install \
	numpy scipy pillow beautifulsoup4 \
	pandas PyYAML jsonlines pytest

RUN /home/${USER_NAME}/.conda/condabin/conda run -n rebuilt pip install \
    git+https://github.com/impresso/dask_k8.git \
    boto \
    boto3 \
    docopt \
    "kubernetes>=9.0.0,<10" \
    "urllib3>1.21.1<1.25" \
    "opencv-python>=3.4,<=4" \
    smart_open \
    "s3fs>=2023.3.0" \
    jupyter

# Set the working directory
WORKDIR /home/$USER_NAME/impresso_pycommons

# Add local impresso_pycommons
COPY . .

# Change ownership of the copied files to the new user and group
RUN chown -R ${USER_NAME}:${GROUP_NAME} /home/${USER_NAME}/impresso_pycommons

# Switch to the new user
USER $USER_NAME

RUN /home/$USER_NAME/.conda/condabin/conda run -n rebuilt pip install -e .

CMD ["sleep", "infinity"]
