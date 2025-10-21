# Base Flink 1.19 image (more stable for CDC connectors)
# CDC connectors are well-tested with Flink 1.18-1.19
FROM flink:1.19-scala_2.12-java17

# Switch to root to install dependencies
USER root

# Install Python3, pip, and tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip wget telnet && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink compatible with Flink 1.19
RUN pip3 install apache-flink==1.19.0 pipreqs

# Set working directory
WORKDIR /opt/flink/workspace

# Copy project files
COPY . .

# ============================================================
# ✅ Download connectors for Flink 1.19 (tested & stable)
# ============================================================

# MySQL CDC Connector (stable with Flink 1.19)
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mysql-cdc/3.0.1/flink-sql-connector-mysql-cdc-3.0.1.jar

# PostgreSQL CDC Connector (stable with Flink 1.19)
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-postgres-cdc/3.0.1/flink-sql-connector-postgres-cdc-3.0.1.jar

# PostgreSQL JDBC Driver (for JDBC sink)
RUN wget -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

# Flink JDBC Connector (for generic JDBC sinks) - version 3.2.0-1.19
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar

# ============================================================
# Install Python requirements
# ============================================================
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Switch back to Flink user
USER flink

# Keep container running (for interactive testing)
CMD ["sleep", "infinity"]