# project dbt
## Make an OLTP:
1. make database at postgres named northwind
dbase name: northwind
host : localhost
user : postgres
type : postgres
passw : 12345678
port : 5432

2. locate csv folder at C:/User/user/project_2
3. Upload the csv data to database postgres, run the python file at init_db:
```
init.py
```

### How to run dbt?

create a virtual env of python first :
```
py -m venv /Users/user/venv_dskola
venv_dskola/scripts/activate
```
1. build the postgres image
```
docker build -t northwind -f Dockerfile.postgres .
```
2. run postgres container
if you want to check the result on local (uncomment first the EXPOSE command on dockerfile postgres)
```
docker run -d -p 5432:5432 --name postgres northwind
```
3. debug your dbt connection
```
dbt debug --profiles-dir ./ --project-dir northwind_project
```
4. run dbt model
```
dbt run --profiles-dir ./ --project-dir northwind_project
```
5. generate dbt docs
```
dbt docs generate --profiles-dir ./ --project-dir northwind_project
```
6. serve dbt docs
```
dbt docs serve --profiles-dir ./ --project-dir northwind_project
```