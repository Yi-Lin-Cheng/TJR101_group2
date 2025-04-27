FROM python:3.13-slim-bullseye

ENV TZ=Asia/Taipei
# 讓container認得外面的MySQL主機
ENV API_HOST=http://host.docker.internal:3306 

RUN apt-get update && \
    apt-get install bash git zsh vim curl wget zip make procps gcc python3-dev -y && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    echo "Y" | sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"

RUN pip install --upgrade pip && \
    pip install poetry

ENV PYTHONPATH=/workspaces/TJR101_group2/src