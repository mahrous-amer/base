FROM python:3.11

ARG service_dir

COPY ../Python/lib /app/lib/

COPY ./Python/requirements.txt /app/lib/

WORKDIR /app/lib

RUN echo "installing apt and pip deps" \
			&& apt-get -y -q update \
			&& DEBIAN_FRONTEND=noninteractive \
			&& apt-get -y -q --no-install-recommends install $(cat aptfile) \
			&& pip install --upgrade pip \
			&& echo $(pwd && ls) && pip install -r requirements.txt

COPY $service_dir /app/

WORKDIR /app

RUN if [ -n "$service_dir" ] ; then \
			echo "installing apt and pip deps" \
			&& apt-get -y -q update \
			&& DEBIAN_FRONTEND=noninteractive \
			&& apt-get -y -q --no-install-recommends install $(cat aptfile) \
			&& pip install -r requirements.txt; \
		fi

ENV PYTHONPATH="/app"

ENTRYPOINT ["/usr/local/bin/python3"]

CMD ["bin/main.py"]
