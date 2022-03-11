FROM accent/python-mssql
RUN mkdir /webapps
WORKDIR /webapps
RUN pip install -U pip setuptools
RUN pip install pyodbc \
                requests \
                pysftp \
                azure.keyvault.secrets \
                azure.identity \ 
                python-dotenv 
ADD . /webapps/
CMD ["python", "aumento.py"]
