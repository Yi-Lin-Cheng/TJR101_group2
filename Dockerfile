FROM python:3.13-slim-bullseye

WORKDIR / workspaces

ENV TZ=Asia/Taipei

# 讓container認得外面的MySQL主機
ENV API_HOST=http://host.docker.internal:3306 

# 設定環境變數路徑
ENV PYTHONPATH=/path/to/src

# 安裝基本工具與開發相關套件
RUN apt-get update && \
    apt-get install -y \
        bash \
        git \
        zsh \
        vim \
        curl \
        wget \
        zip \
        make \
        procps \
        gcc \
        python3-dev \
        gnupg \
        ca-certificates \
        fonts-liberation \
        libnss3 \
        libxss1 \
        libatk-bridge2.0-0 \
        libgtk-3-0 && \

    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    dpkg -i google-chrome-stable_current_amd64.deb || apt-get -f install -y && \
    rm google-chrome-stable_current_amd64.deb && \

    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 安裝 oh-my-zsh（非必要但你有需求就保留）
RUN echo "Y" | sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" || true

RUN pip install --upgrade pip
RUN pip install poetry selenium webdriver-manager
