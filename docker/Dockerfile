from ubuntu:16.04
# general deps
# bomb early if pathes are broken.
RUN apt-get update && apt-get install -y python python3 python-virtualenv python3-venv python-pip python3-pip vim git ca-certificates libhdf5-dev
# r-deps
RUN echo "deb http://cran.rstudio.com/bin/linux/ubuntu xenial/" >> /etc/apt/sources.list
RUN gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-key E084DAB9
RUN gpg -a --export E084DAB9 | apt-key add -
RUN apt-get update
RUN apt-get install -y r-base
RUN update-ca-certificates
RUN useradd --create-home --shell /bin/bash testuser
RUN echo 'testuser:testpass' | chpasswd
USER testuser
WORKDIR /home/testuser
RUN mkdir venvs
COPY ./requirements.txt ./requirements.txt
COPY ./test_requirements.txt ./test_requirements.txt
COPY ./r_requirements.txt ./r_requirements.txt
RUN virtualenv venvs/venv27
RUN python3 -m venv venvs/venv35
# upgrading pip doesn't upgrade the system pip.
RUN /bin/bash -c "source venvs/venv27/bin/activate && pip install --upgrade pip && deactivate"
RUN /bin/bash -c "source venvs/venv35/bin/activate && pip install --upgrade pip && deactivate"
RUN /bin/bash -c "source venvs/venv27/bin/activate && cat requirements.txt r_requirements.txt test_requirements.txt | pip install -r /dev/stdin && deactivate"
RUN /bin/bash -c "source venvs/venv35/bin/activate && cat requirements.txt r_requirements.txt test_requirements.txt | pip install -r /dev/stdin  && deactivate"
