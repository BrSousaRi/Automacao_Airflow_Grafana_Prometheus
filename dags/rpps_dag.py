from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
import time
import re
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

# Configurações
URL = "https://www.gov.br/previdencia/pt-br/assuntos/rpps/estatisticas-e-informacoes-dos-rpps-1/estatisticas-informacoes-dos-rpps"
LOCAL_FOLDER = "/tmp/downloads_rpps"
ARQUIVOS = [
    "2 - Colegiado Deliberativo do RPPS",
    "3 - Comitê de Investimento do RPPS",
    "4 - Conselho de Fiscalização do RPPS",
    "6 - Forma de Gestão e Acessoramento",
    "8 - Gestão dos Recursos do RPPS",
]

# Credenciais Azure
AZURE_STORAGE_ACCOUNT = Variable.get(
    "AZURE_STORAGE_ACCOUNT", default_var="4260900tceanalyticsdev"
)
AZURE_TENANT_ID = Variable.get("AZURE_TENANT_ID", default_var="")
AZURE_CLIENT_ID = Variable.get("AZURE_CLIENT_ID", default_var="")
AZURE_CLIENT_SECRET = Variable.get("AZURE_CLIENT_SECRET", default_var="")
CONTAINER_NAME = Variable.get("AZURE_CONTAINER_NAME", default_var="tce")
BLOB_FOLDER_PATH = Variable.get(
    "AZURE_BLOB_PATH", default_var="000-dado-bruto/000-fonte-externa/016-mpc-rpps"
)


def log(mensagem, tipo="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{tipo}] {mensagem}")


@dag(
    dag_id="rpps_download_upload",
    start_date=datetime(2025, 1, 1),
    schedule="0 8 * * *",
    catchup=False,
    default_args={
        "owner": "data_team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["rpps", "azure", "download"],
)
def rpps_pipeline():
    @task()
    def preparar_ambiente():
        """Limpa e prepara pasta de downloads"""
        import shutil

        if os.path.exists(LOCAL_FOLDER):
            try:
                shutil.rmtree(LOCAL_FOLDER)
                log("Pasta limpa")
            except:
                pass
        os.makedirs(LOCAL_FOLDER, exist_ok=True)
        return LOCAL_FOLDER

    @task()
    def baixar_arquivos_rpps(pasta_downloads: str):
        """Download dos arquivos do site RPPS"""
        log("=== INICIANDO DOWNLOADS ===")

        options = webdriver.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")

        prefs = {
            "download.default_directory": pasta_downloads,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
        }
        options.add_experimental_option("prefs", prefs)

        driver = webdriver.Chrome(options=options)
        wait = WebDriverWait(driver, 15)

        try:
            driver.get(URL)
            time.sleep(6)

            link = driver.find_element(By.LINK_TEXT, "Carteira")
            log("Link Carteira encontrado")
            driver.get(link.get_attribute("href"))
            time.sleep(6)

            pastas = driver.find_elements(By.CSS_SELECTOR, "a[href*='/s/']")
            pastas_com_data = []

            for p in pastas:
                match = re.search(
                    r"Atualizado_ate_(\d{2})_(\d{2})_(\d{4})", p.text.strip()
                )
                if match:
                    dia, mes, ano = match.groups()
                    try:
                        data = datetime(int(ano), int(mes), int(dia))
                        pastas_com_data.append(
                            (data, p.text.strip(), p.get_attribute("href"))
                        )
                    except ValueError:
                        continue

            if not pastas_com_data:
                raise Exception("Nenhuma pasta encontrada")

            pasta_info = sorted(pastas_com_data, key=lambda x: x[0])[-1]
            log(f"Pasta: {pasta_info[1]}")

            driver.get(pasta_info[2])
            time.sleep(6)
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "tr[data-file]"))
            )
            time.sleep(3)

            def baixar(xpath_busca):
                try:
                    linha = driver.find_element(By.XPATH, xpath_busca)
                    nome = linha.get_attribute("data-file")
                    log(f"Baixando: {nome}")

                    tres_pontos = linha.find_element(By.CSS_SELECTOR, "a.action")
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center'});", tres_pontos
                    )
                    time.sleep(1)

                    try:
                        tres_pontos.click()
                    except:
                        driver.execute_script("arguments[0].click();", tres_pontos)

                    time.sleep(2)
                    opcao_baixar = wait.until(
                        EC.element_to_be_clickable(
                            (By.XPATH, "//a[@data-action='Download']")
                        )
                    )
                    opcao_baixar.click()
                    time.sleep(2)
                    log(f"✓ {nome}", "OK")
                except Exception as e:
                    log(f"✗ Erro: {e}", "ERRO")

            linhas_carteira = driver.find_elements(
                By.XPATH, "//tr[contains(@data-file, 'Carteira')]"
            )
            log(f"Carteiras encontradas: {len(linhas_carteira)}")

            for linha in linhas_carteira:
                baixar(f"//tr[@data-file='{linha.get_attribute('data-file')}']")

            for arquivo in ARQUIVOS:
                baixar(f"//tr[starts-with(@data-file, '{arquivo}')]")

            log("Aguardando downloads")
            time.sleep(15)

            for arquivo in os.listdir(pasta_downloads):
                if arquivo.endswith(".crdownload"):
                    try:
                        os.rename(
                            os.path.join(pasta_downloads, arquivo),
                            os.path.join(
                                pasta_downloads, arquivo.replace(".crdownload", "")
                            ),
                        )
                    except FileExistsError:
                        os.remove(os.path.join(pasta_downloads, arquivo))

            total = len(os.listdir(pasta_downloads))
            log(f"Downloads concluídos: {total} arquivos")

        finally:
            driver.quit()

        return pasta_downloads

    @task()
    def upload_para_azure(pasta_downloads: str):
        """Upload dos CSVs para Azure Storage"""
        log("=== INICIANDO UPLOAD AZURE ===")

        csv_files = [
            f for f in os.listdir(pasta_downloads) if f.lower().endswith(".csv")
        ]

        if not csv_files:
            log("Nenhum CSV encontrado", "AVISO")
            return

        log(f"CSVs para upload: {len(csv_files)}")

        account_url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net"
        credential = ClientSecretCredential(
            tenant_id=AZURE_TENANT_ID,
            client_id=AZURE_CLIENT_ID,
            client_secret=AZURE_CLIENT_SECRET,
        )
        blob_service = BlobServiceClient(account_url=account_url, credential=credential)

        for file_name in csv_files:
            try:
                local_path = os.path.join(pasta_downloads, file_name)
                blob_full_path = f"{BLOB_FOLDER_PATH}/{file_name}"

                blob_client = blob_service.get_blob_client(
                    container=CONTAINER_NAME, blob=blob_full_path
                )

                with open(local_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)

                log(f"✓ {file_name}", "OK")
            except Exception as e:
                log(f"✗ Erro {file_name}: {e}", "ERRO")

        log(f"Upload concluído: {len(csv_files)} arquivos")

    @task()
    def limpar_arquivos(pasta_downloads: str):
        """Remove pasta temporária"""
        import shutil

        if os.path.exists(pasta_downloads):
            try:
                shutil.rmtree(pasta_downloads)
                log("Pasta removida")
            except:
                log("Não foi possível remover pasta", "AVISO")

    pasta = preparar_ambiente()
    downloads = baixar_arquivos_rpps(pasta)
    upload = upload_para_azure(downloads)
    limpar = limpar_arquivos(downloads)

    pasta >> downloads >> upload >> limpar


rpps_pipeline()

