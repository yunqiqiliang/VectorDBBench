ARG BASE_IMAGE=python:3.11-slim
FROM ${BASE_IMAGE} AS builder-image

RUN apt-get update && apt-get install -y git

RUN apt-get update

COPY install/requirements_py3.11.txt .
RUN pip3 install -U pip
# RUN pip3 install  -r requirements_py3.11.txt 

# 运行阶段
FROM ${BASE_IMAGE}
RUN apt-get update && apt-get install -y git
COPY --from=builder-image /usr/local/bin /usr/local/bin
COPY --from=builder-image /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

WORKDIR /opt/code
EXPOSE 8501
COPY . .
COPY .env.example .env
ENV PYTHONPATH=/opt/code

# 安装可编辑模式依赖
RUN pip3 install -e '.[test]'
RUN pip3 install -e '.[clickzettalakehouse]'
RUN pip3 install -e '.[qdrant]'
RUN pip3 install -e '.[pgvector]'


ENTRYPOINT ["init_bench"]

# ENTRYPOINT ["python3", "-m", "vectordb_bench"]