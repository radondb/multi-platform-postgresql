# Build container
FROM python:3.10-slim

ENV TZ=Asia/Shanghai
RUN mkdir -pv /src

WORKDIR /src

COPY postgres /src

RUN python -m pip install --trusted-host mirrors.aliyun.com --upgrade pip \
		-i http://mirrors.aliyun.com/pypi/simple/
RUN python -m pip install --trusted-host mirrors.aliyun.com --no-cache-dir \
		Kubernetes==21.7.0 kopf==1.35.5 wrapt paramiko apscheduler \
		-i http://mirrors.aliyun.com/pypi/simple/

#ENTRYPOINT ["kopf", "run", "--standalone", "-A", "--liveness=http://0.0.0.0:8080/healthz"]
#CMD ["postgres.py"]
# run postgres.py files in all subdirectories (excluding the current directory)
ENTRYPOINT ["sh", "-c", "find **/ -type f -name 'postgres.py' -printf '%p ' | xargs -r kopf run --standalone -A --liveness=http://0.0.0.0:8080/healthz"]
#CMD ["postgres.py"]
#CMD ["v1-2-0/postgres.py", "v1-3-0/postgres.py"]
