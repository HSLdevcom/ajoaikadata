FROM bytewax/bytewax:0.17.2-python3.11

COPY requirements.txt /tmp/requirements.txt

RUN /venv/bin/pip install -r /tmp/requirements.txt
