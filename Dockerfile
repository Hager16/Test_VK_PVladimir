FROM apache/airflow:2.10.1

USER root

RUN apt-get update && \
    apt-get install -y curl && \
    mkdir -p /usr/lib/jvm && \
    curl -L -o openjdk-11.tar.gz https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.11+9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.11_9.tar.gz && \
    tar -xzf openjdk-11.tar.gz -C /usr/lib/jvm && \
    rm openjdk-11.tar.gz

# Устанавливаем переменные среды
ENV JAVA_HOME=/usr/lib/jvm/jdk-11.0.11+9
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

# Установка необходимых пакетов, если нужно
RUN pip install apache-airflow-providers-apache-spark



