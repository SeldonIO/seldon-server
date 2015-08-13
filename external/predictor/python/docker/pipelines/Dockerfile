FROM continuumio/anaconda

RUN pip install filechunkio

ADD ./python-modules /python-modules/seldon

ENV PYTHONPATH $PYTHONPATH:/python-modules

# Define default command.
CMD ["bash"]
