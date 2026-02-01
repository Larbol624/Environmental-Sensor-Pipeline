FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    netcat-openbsd \
    openjdk-21-jdk-headless\
    && rm -rf /var/lib/apt/lists/* 
    
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH


RUN pip install --no-cache-dir notebook pandas numpy matplotlib kafka-python confluent-kafka pyspark


WORKDIR /workspace

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--no-browser"]