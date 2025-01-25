pip install meltano

meltano init meu_projeto
cd meu_projeto

meltano add extractor tap-postgres
meltano add loader target-csv

extractors:
- name: tap-postgres
  config:
    host: "host"
    port: "porta"
    database: "nome_do_banco"
    user: "usuario"
    password: "senha"
loaders:
- name: target-csv
  config:
    destination_path: "/path/to/save/csv"

