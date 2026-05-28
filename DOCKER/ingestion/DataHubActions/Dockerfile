FROM acryldata/datahub-actions:head-slim
WORKDIR /opt/federation-plugin
COPY federation_forwarder.py setup.py ./
RUN pip install --no-cache-dir -e .
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
