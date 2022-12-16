FROM grafana/grafana

COPY ./prometheus_ds.yml /etc/grafana/provisioning/dashboards/
COPY ./prometheus.yml  /etc/grafana/provisioning/datasources/
COPY ./*.json /var/lib/grafana/dashboards/
ENV AGENT_URL=http://127.0.0.1:9080
ENV DP_URL=http://127.0.0.1:9081