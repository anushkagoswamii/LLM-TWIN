FROM --platform=linux/amd64 python:3.11

WORKDIR /social_sensing

COPY .env logger.py constant.py helper.py data_crawler.py feature_extractor.py instruction_generator.py requirements.txt /social_sensing/


# INSTALLING CHROME
RUN apt-get update \
    && apt-get clean \
    && apt-get install -y wget \
    && apt-get install -y gnupg \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get -y install google-chrome-stable \
    && apt-get clean


# Requirements
RUN pip install --no-cache-dir -r requirements.txt

# MAIN
CMD ["python3", "data_crawler.py"]
