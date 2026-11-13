FROM spark:4.0.1-scala2.13-java21-python3-ubuntu
USER root
# Same uid/gid as the Airflow image (50000:0) so both can manage files in the shared lake volume.
RUN useradd -u 50000 -g 0 -m -d /home/wikipulse wikipulse
COPY jobs/ /opt/jobs/
USER 50000
WORKDIR /opt/jobs
ENV SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1
CMD ["bash", "-c", "/opt/spark/bin/spark-submit --master local[2] --driver-memory ${SPARK_DRIVER_MEMORY:-1536m} --packages ${SPARK_KAFKA_PACKAGE} --conf spark.jars.ivy=/opt/ivy --py-files /opt/jobs/transforms.py /opt/jobs/stream_job.py"]
