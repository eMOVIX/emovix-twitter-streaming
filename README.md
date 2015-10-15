# emovix-twitter-streaming
Application to fetch information from the public Twitter streaming API

## Prerequisites

 - Python 2.7
 - git
 - pip
 - virtualenv


## Installation

    git clone https://github.com/eMOVIX/emovix-twitter-streaming.git
    cd emovix-twitter-streaming
    virtualenv venv
    source venv/bin/activate
    pip install -r requirements.txt

## Configuration

Add your Twitter API credentials and MongoDB database name to the configuration file:

    vim config.json

## Usage

    python emovix-twitter-streaming.py
