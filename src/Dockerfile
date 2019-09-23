FROM amsterdam/docker_python:latest
MAINTAINER datapunt@amsterdam.nl

# Airflow
ARG AIRFLOW_VERSION=1.10.4
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}


# Install required Python packages
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

COPY script/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg

COPY dags ${AIRFLOW_USER_HOME}/dags/
COPY plugins ${AIRFLOW_USER_HOME}/plugins/

WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint
