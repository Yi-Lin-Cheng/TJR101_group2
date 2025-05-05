FROM python:3.13-slim-bullseye

ENV TZ=Asia/Taipei

# 讓container認得外面的MySQL主機
ENV API_HOST=http://host.docker.internal:3306 

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
        libgtk-3-0 \
        libgbm1 \
        libasound2 \
        libx11-xcb1 \
        libxcomposite1 \
        libxcursor1 \
        libxdamage1 \
        libxrandr2 \
        libdrm2 \
        libxfixes3 \
        libxi6 \
        libgl1 && \
    rm -rf /var/lib/apt/lists/*

# 安裝指定版本的 Chrome for Testing 和 Chromedriver
RUN apt-get update && apt-get install -y unzip && \
    wget -O chrome-linux64.zip https://storage.googleapis.com/chrome-for-testing-public/136.0.7103.49/linux64/chrome-linux64.zip && \
    wget -O chromedriver-linux64.zip https://storage.googleapis.com/chrome-for-testing-public/136.0.7103.49/linux64/chromedriver-linux64.zip && \
    unzip chrome-linux64.zip && \
    unzip chromedriver-linux64.zip && \
    mv chrome-linux64 /opt/chrome && \
    ln -s /opt/chrome/chrome /usr/local/bin/google-chrome && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/ && \
    chmod +x /usr/local/bin/google-chrome /usr/local/bin/chromedriver && \
    rm -rf chrome-linux64.zip chromedriver-linux64.zip

ENV PATH="$PATH:/usr/local/bin"

RUN echo "Y" | sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" || true

RUN pip install --upgrade pip
RUN pip install poetry