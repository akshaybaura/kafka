FROM python:3.8

ENV TDIR /home/app

RUN mkdir -p $TDIR
COPY ./json_consumer.py ./json_producer.py ./config_json.ini ./requirements.txt ./routes.tsv $TDIR/
RUN cd $TDIR/ && pip install -r requirements.txt
RUN apt-get update && apt-get install -y vim

CMD /bin/bash