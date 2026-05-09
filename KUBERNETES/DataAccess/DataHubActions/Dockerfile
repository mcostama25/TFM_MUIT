FROM acryldata/datahub-actions:698bd78-slim

USER root
# RUN apt-get update && apt-get install -y python3-pip

COPY setup.py /app/event-logger/setup.py
COPY src/     /app/event-logger/src/

RUN chown -R datahub:datahub /app/event-logger

WORKDIR /app/event-logger
RUN /home/datahub/.venv/bin/pip3 install . && \
    mkdir -p /etc/datahub/actions/conf

USER datahub