# Python base image
FROM python:3.9-slim

# A layer for Java (needed for Spark)
COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8
ENV JAVA_HOME /usr/local/openjdk-8

# Prepare
RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1 && \
    apt-get update && apt-get install curl -y && \
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs && \
    npm install -g aws-cdk && \
    mkdir -p /app

# Set /app as the working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN python3 -m pip install --upgrade pip && pip install -r requirements.txt

# Copy all code and run tests
COPY . .
RUN pyclean . && pytest tests

# Entry
ENTRYPOINT [ "/bin/bash" ] 