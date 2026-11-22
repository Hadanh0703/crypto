FROM spark:3.5.8-java17-python3

WORKDIR /opt/app

USER root

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir python-dotenv==1.0.1

COPY src ./src

ENV PYTHONPATH=/opt/app/src

USER spark

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
