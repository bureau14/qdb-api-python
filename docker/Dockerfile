ARG PYTHON_VERSION

FROM python:${PYTHON_VERSION}

RUN apt-get update \
        && apt-get install -y cmake \
        && apt-get clean

RUN pip install pipenv
