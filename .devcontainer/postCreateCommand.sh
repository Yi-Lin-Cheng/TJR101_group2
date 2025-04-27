#!/bin/bash
set -x

export PYTHONPATH=$(pwd)/src
poetry config virtualenvs.in-project true --local
poetry install

sed -i 's/ZSH_THEME="robbyrussell"/ZSH_THEME="xiong-chiamiov-plus"/' /root/.zshrc \
&& sed -i '/HIST_STAMPS="mm\/dd\/yyyy"/s/^#*//;s/mm\/dd\/yyyy/yyyy-mm-dd/' /root/.zshrc

zsh
