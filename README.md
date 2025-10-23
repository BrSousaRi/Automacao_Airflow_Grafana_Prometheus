#  RPPS Data Pipeline - TCE

Pipeline de automamação para download e armazenamento de dados do RPPS (Regime Próprio de Previdência Social) usando Apache Airflow e Azure Storage.

##  Descrição

Este projeto automatiza o processo de:
1. Download de arquivos CSV do site do Ministério da Previdência
2. Armazenamento no Azure Blob Storage
3. Organização dos dados para análise

##  Arquitetura
```
┌─────────────────┐
│  Site RPPS      │
│  (Selenium)     │
└────────┬────────┘
         │ Download CSV
         ▼
┌─────────────────┐
│  Airflow DAG    │
│  (Docker)       │
└────────┬────────┘
         │ Upload
         ▼
┌─────────────────┐
│  Azure Storage  │
│  Blob Container │
└─────────────────┘
``` 

##  Tecnologias

- **Apache Airflow 2.8.1** - Orquestração
- **Selenium 4.15.2** - Web scraping
- **Azure Storage Blob 12.19.0** - Armazenamento
- **Docker** - Containerização
- **Python 3.11** - Linguagem base

##  Estrutura do Projeto
```
MPC/
├── dags/
│   └── rpps_dag.py           # DAG principal
├── logs/                     # Logs do Airflow
├── downloads/                # Downloads temporários
├── plugins/                  # Plugins customizados
├── docker-compose.yml        # Configuração Docker
├── Dockerfile.airflow        # Imagem customizada
├── requirements.txt          # Dependências Python
├── .env                      # Variáveis (não versionado)
├── .gitignore               
└── README.md
```

##  Como Executar

### Pré-requisitos
- Docker Desktop instalado
- Git Bash (Windows)

### 1. Configure as variáveis de ambiente

Crie as seguintes variáveis no Airflow UI (Admin → Variables):

| Variável | Descrição |
|----------|-----------|
| `AZURE_TENANT_ID` | Azure Tenant ID |
| `AZURE_CLIENT_ID` | Service Principal Client ID |
| `AZURE_CLIENT_SECRET` | Service Principal Secret |
| `AZURE_STORAGE_ACCOUNT` | Nome da conta de storage (padrão: 4260900tceanalyticsdev) |
| `AZURE_CONTAINER_NAME` | Nome do container (padrão: tce) |
| `AZURE_BLOB_PATH` | Caminho no blob (padrão: 000-dado-bruto/000-fonte-externa/016-mpc-rpps) |

### 2. Inicie o ambiente
```bash
# Cria pastas necessárias
mkdir -p dags logs downloads plugins

# Dá permissões
chmod 777 logs downloads

# Sobe os containers
docker-compose up -d

### 3. Acesse o Airflow
- URL: http://localhost:8080
- User: `admin`
- Password: `admin123`

### 4. Execute a DAG
1. Ative a DAG `rpps_download_upload`
2. Trigger aguarde o schedule

##  Schedule

**Padrão:** Dia 1 de cada mês às 8h da manhã
```python
schedule="0 8 * * *"
```

Para alterar, edite `dags/rpps_dag.py`.

##  Pipeline (Tasks)

1. **preparar_ambiente** - Limpa pasta temporária
2. **baixar_arquivos_rpps** - Download via Selenium
   - Carteira (todos os anos)
   - Colegiado Deliberativo
   - Comitê de Investimento
   - Conselho de Fiscalização
   - Forma de Gestão
   - Gestão dos Recursos
3. **upload_para_azure** - Upload CSVs para Azure
4. **limpar_arquivos** - Remove arquivos temporários

##  Monitoramento

### Logs
```bash
# Scheduler
docker-compose logs airflow-scheduler

# Webserver
docker-compose logs airflow-webserver

# Logs de uma task específica
# Acesse via Airflow UI → DAG → Task → Logs
```

### Status
Acesse: http://localhost:8080/dags/rpps_download_upload/grid

# Verifica erros de import
docker-compose logs airflow-scheduler | grep -i error

# Restart scheduler
docker-compose restart airflow-scheduler
```
### Erro de permissão
```bash
chmod 777 logs downloads
docker-compose restart
```

### Chrome/Selenium não funciona
```bash
# Rebuild com cache limpo
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

##  Arquivos Baixados

O pipeline baixa os seguintes arquivos:
- `1 - Carteira_2013a2020.csv`
- `1.1 - Carteira_2021.csv`
- `1.2 - Carteira_2022.csv`
- `1.3 - Carteira_2023.csv`
- `1.4 - Carteira_2024.csv`
- `1.5 - Carteira_2025.csv`
- `2 - Colegiado Deliberativo.csv`
- `3 - Comitê de Investimento.csv`
- `4 - Conselho de Fiscalização.csv`
- `6 - Forma de Gestão.csv`
- `8 - Gestão dos Recursos.csv`
