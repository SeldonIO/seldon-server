FROM seldonio/pyseldon:%SELDON_PYTHON_PACKAGE_VERSION%

ENV HOME /root
ENV DEBIAN_FRONTEND noninteractive

RUN pip install locustio

ENV SELDON_HOME /home/seldon
ADD ./scripts $SELDON_HOME/scripts

# Define default command.
CMD ["bash"]

