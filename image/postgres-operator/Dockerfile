# Build container
FROM python:3.10-slim

RUN mkdir -pv /src

WORKDIR /src

COPY postgres /src

RUN python -m pip install --trusted-host mirrors.aliyun.com --upgrade pip \
		-i http://mirrors.aliyun.com/pypi/simple/
RUN python -m pip install --trusted-host mirrors.aliyun.com --no-cache-dir \
		Kubernetes kopf wrapt paramiko \
		-i http://mirrors.aliyun.com/pypi/simple/

ENTRYPOINT ["kopf", "run", "--standalone", "-A"]
CMD ["postgres.py"]
