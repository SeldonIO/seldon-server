FROM ubuntu:latest

RUN apt-get update && apt-get -y install cron rsyslog

# Add crontab file in the cron directory
ADD scripts/crontab /etc/cron.d/rm-old-logs

# Give execution rights on the cron job
RUN chmod 0600 /etc/cron.d/rm-old-logs

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

ENV SELDON_HOME /home/seldon

RUN mkdir -p  "${SELDON_HOME}"

ADD scripts/rm_old_logs.sh $SELDON_HOME/rm_old_logs.sh

# Run the command on container startup
#fix bug in docker hard links to cronjob stops cron working
CMD touch /etc/cron.d/rm-old-logs && cron -L15 && tail -f /var/log/cron.log
