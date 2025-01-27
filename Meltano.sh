pip install meltano
meltano init my_project
cd my_project
meltano add extractor tap-csv
meltano add loader target-postgres
