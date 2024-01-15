# Pulling Ubuntu image
FROM ubuntu:20.04

# Adding crontab to the appropiate location
ADD crontab /etc/cron.d/cronjob
ADD load_data.py .

# Updating packages and installing cron
RUN touch /var/log/cron.log && \
    apt-get update && apt-get install cron python3 python3-psycopg2 -y && \
    chmod +x load_data.py && \
    chmod 0644 /etc/cron.d/cronjob && \
    crontab /etc/cron.d/cronjob

# Creating entry point for cron
CMD cron && tail -f /var/log/cron.log
