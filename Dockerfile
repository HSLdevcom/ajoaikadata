FROM bytewax/bytewax:0.17.2-python3.11

COPY requirements.txt /tmp/requirements.txt
COPY entrypoint.sh /bytewax/entrypoint.sh

RUN /venv/bin/pip install -r /tmp/requirements.txt
