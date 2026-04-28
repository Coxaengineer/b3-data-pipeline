# b3-data-pipeline

Pipeline de Engenharia de Dados para cotações da B3

## Arquitetura

- **Bronze**: Ingestão diária via API brapi.dev → JSON
- **Silver**: Limpeza e tipagem → Parquet
- **Gold**: Métricas e rankings → Parquet

## Tecnologias

- Python
- Apache Airflow
- PySpark
- Docker

## Como rodar

```bash
docker compose up -d
```

Acessa http://localhost:8080 (admin/admin)