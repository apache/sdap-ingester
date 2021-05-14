FROM python:3

COPY /config_operator /config_operator/config_operator
COPY /setup.py /config_operator/setup.py
COPY /requirements.txt /config_operator/requirements.txt
COPY /README.md /config_operator/README.md

RUN cd /config_operator && pip install .
COPY /config_operator/main.py /main.py

CMD ["kopf",  "run",  "/main.py",  "--verbose"]
