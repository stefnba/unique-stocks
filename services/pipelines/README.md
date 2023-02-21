# Airflow

## Setup

Create and activate virtual enviornment (python)

```bash
conda create -p .conda python=3.10
conda activate "$PWD/.conda"
```

Install packages

```bash
make packages
```

Initialize containers

```bash
docker-compose up airflow-init
```

Start containers

```bash
docker-compose up
```
