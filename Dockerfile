FROM continuumio/miniconda3:4.9.2

COPY environment.yml .
RUN conda env update -f environment.yml --prune