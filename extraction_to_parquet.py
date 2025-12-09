import argparse
import json
import os
import logging
import pandas as pd

# ----------------- CONFIGURAÇÃO -----------------
EXTRACTION_FOLDER = "extraction"
OUTPUT_FOLDER = "parquet"
LOGS_FOLDER = "logs"

LOG_FILE = os.path.join(LOGS_FOLDER, "json_to_parquet.log")

# Logging
logging.basicConfig(
    filename=LOG_FILE,
    encoding="utf-8",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(console)

# ----------------- UTILITÁRIOS -----------------
def load_json(file_path):
    """Carrega o conteúdo de um arquivo JSON"""
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def ensure_folder(path):
    """Cria a pasta caso não exista"""
    os.makedirs(path, exist_ok=True)

# ----------------- PROCESSAMENTO -----------------
def merge_json_to_parquet(start, end, output_file):
    """Une arquivos JSON da extração em um único Parquet sem transformação"""

    logging.info("Iniciando conversão JSON para Parquet...")
    all_movies = []

    for i in range(start, end + 1):
        file_path = os.path.join(EXTRACTION_FOLDER, f"tmdb_movies_{i}.json")

        if not os.path.exists(file_path):
            logging.warning(f"Arquivo não encontrado: {file_path}")
            continue

        logging.info(f"Lendo arquivo: {file_path}")
        data = load_json(file_path)
        all_movies.extend(data)

    if not all_movies:
        logging.warning("Nenhum filme encontrado no intervalo informado.")
        return

    # Criar DataFrame sem transformação — dados RAW
    df = pd.DataFrame(all_movies)

    ensure_folder(OUTPUT_FOLDER)
    output_path = os.path.join(OUTPUT_FOLDER, output_file)

    logging.info(f"Salvando Parquet em: {output_path}")
    df.to_parquet(output_path, index=False)

    logging.info("Conversão finalizada com sucesso.")

# ----------------- EXECUÇÃO -----------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Converter arquivos JSON RAW do TMDB em Parquet RAW")

    parser.add_argument("--start", type=int, required=True, help="Arquivo inicial (ex: 1)")
    parser.add_argument("--end", type=int, required=True, help="Arquivo final (ex: 30)")
    parser.add_argument("--output", type=str, default="movies_raw.parquet", help="Nome do Parquet final")

    args = parser.parse_args()

    merge_json_to_parquet(args.start, args.end, args.output)
