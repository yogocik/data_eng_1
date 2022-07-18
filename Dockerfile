FROM python:3.8

WORKDIR /usr/dwh-challenge 

COPY requirements.txt .

COPY data ./data 

COPY solution ./solution

RUN pip install -r requirements.txt

ENTRYPOINT [ "python" ]

CMD ["solution/main.py"]

