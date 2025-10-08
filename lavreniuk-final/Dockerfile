# Start with your base image
FROM apache/airflow:2.7.3-python3.10

# Switch to root to install system packages
USER root

# Install Java (OpenJDK) and other dependencies.
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set the correct JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

# Add the Java bin directory to the PATH for the airflow user
RUN echo "export PATH=$PATH:$JAVA_HOME/bin" >> /etc/profile.d/java.sh

# Change ownership of the Java installation directory to the airflow user.
# The path is now correct and will not result in a "No such file or directory" error.
RUN chown -R airflow: /usr/lib/jvm/java-11-openjdk-arm64

# Switch back to the airflow user
USER airflow

# Continue with your other installations (e.g., requirements.txt)
COPY req.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt