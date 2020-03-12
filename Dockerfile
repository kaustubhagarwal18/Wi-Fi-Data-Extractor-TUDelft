FROM python

RUN apt install python-pip

RUN pip install kafka-python

RUN pip install pandas

RUN pip install flatten_json

RUN  pip install hashlib

RUN apt-get install curl

RUN  pip install hmac

COPY . /src


