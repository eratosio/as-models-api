FROM ubuntu:18.04
# python host deps for as-models
RUN apt-get -y update && \
	apt-get -y install git python3 python3-venv python3-pip ca-certificates libhdf5-dev && \
	ln -s /usr/bin/python3 /usr/bin/python && \
	update-ca-certificates && \
	USE_SETUPCFG=0 HDF5_INCDIR=/usr/include/hdf5/serial HDF5_LIBDIR=/usr/lib/x86_64-linux-gnu/hdf5/serial pip3 install netCDF4

# r-deps
RUN apt-get -y update && \
	apt-get -y install apt-transport-https software-properties-common && \
	apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
	add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/' && \
	add-apt-repository 'deb http://mirror.aarnet.edu.au/pub/ubuntu/archive/ bionic-backports main restricted universe' && \
	apt-get -y update && \
	export TZ=UTC && \
	DEBIAN_FRONTEND=noninteractive apt -y install r-base && \
	pip3 install rpy2==3.3.3 && \
	apt-get install -y --allow-unauthenticated libcurl4-openssl-dev libssl-dev libxml2-dev libnetcdf-dev
# the following is to enable full Senaps base-iamge behaviour - include helper libs for R:
#RUN	R -e "install.packages(c('tidyverse', 'rjson', 'httr', 'zoo', 'readr', 'XML', 'xml2', 'RCurl', 'reshape2', 'gsubfn', 'lubridate', 'plyr', 'bitops', 'ncdf4'), repos='http://cran.rstudio.com/')" && \
#	python -OO /opt/precompile.py
RUN useradd --create-home --shell /bin/bash testuser
RUN echo 'testuser:testpass' | chpasswd
USER testuser
WORKDIR /home/testuser
RUN mkdir venvs
COPY ./requirements.txt ./requirements.txt
COPY ./test_requirements.txt ./test_requirements.txt
COPY ./r_requirements.txt ./r_requirements.txt
RUN python3 -m venv venvs/venv35
# upgrading pip doesn't upgrade the system pip.
RUN /bin/bash -c "source venvs/venv35/bin/activate && pip install --upgrade pip && deactivate"
RUN /bin/bash -c "source venvs/venv35/bin/activate && cat requirements.txt r_requirements.txt test_requirements.txt | pip install -r /dev/stdin  && deactivate"
