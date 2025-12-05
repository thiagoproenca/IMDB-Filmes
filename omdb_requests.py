import requests
import json
import logging
import os

# ----------------- CONFIGURAÇÃO -----------------
OMDB_API_KEYS = ["58003cc5", "dabeb3cd", "9d6e27a9", "505ba252", "1f81f7c5"]
omdb_key_index = 0  # índice da chave OMDB atual

# Caminhos dos arquivos
EXTRACTION_FOLDER = "extraction"
LOGS_FOLDER = "logs"

CHECKPOINT_FILE = os.path.join(EXTRACTION_FOLDER, "checkpoint_omdb.json")
TMDB_MOVIES_FOLDER = EXTRACTION_FOLDER  # Agora é a pasta onde todos os arquivos tmdb_movies_*.json estão localizados
LOG_FILE = os.path.join(LOGS_FOLDER, "omdb_requests.log")

# Logging
logging.basicConfig(
    filename=LOG_FILE,
    encoding='utf-8',
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

def save_json(data, file_path):
    """Salva os dados em um arquivo JSON"""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Cria a pasta caso não exista
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ----------------- OMDB COM ROTAÇÃO DE CHAVES -----------------
def movie_omdb_search(imdb_id):
    global omdb_key_index
    if not imdb_id:
        return {}

    while omdb_key_index < len(OMDB_API_KEYS):
        key = OMDB_API_KEYS[omdb_key_index]
        url = f"http://www.omdbapi.com/?i={imdb_id}&apikey={key}"
        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            data = res.json()
            
            # sucesso
            ratings_dict = {"imdb": None, "rotten_tomatoes": None, "metacritic": None}
            for r in data.get("Ratings", []):
                source = r.get("Source")
                value = r.get("Value")
                if source == "Internet Movie Database":
                    ratings_dict["imdb"] = value
                elif source == "Rotten Tomatoes":
                    ratings_dict["rotten_tomatoes"] = value
                elif source == "Metacritic":
                    ratings_dict["metacritic"] = value
            return {"awards": data.get("Awards"), "ratings": ratings_dict}
        except requests.RequestException:
            if omdb_key_index == len(OMDB_API_KEYS)-1:
                logging.warning(f"Limite diário atingido para a chave OMDB {key}...")
                omdb_key_index += 1
                continue
            logging.warning(f"Limite diário atingido para a chave OMDB {key}, trocando de chave...")
            omdb_key_index += 1

    # todas as chaves esgotadas
    raise RuntimeError("Todas as chaves OMDB atingiram o limite diário, parada da coleta.")

# Função para carregar o checkpoint de progresso
def load_checkpoint():
    return load_json(CHECKPOINT_FILE) or {"last_imdb_id": None, "last_file_index": 1}

def save_checkpoint(checkpoint):
    save_json(checkpoint, CHECKPOINT_FILE)

# Função para carregar os filmes de um arquivo específico TMDB
def load_tmdb_movies(file_path):
    return load_json(file_path)

# Função para obter o próximo arquivo TMDB (se existir)
def get_next_tmdb_file(last_file_index):
    all_files = [f for f in os.listdir(TMDB_MOVIES_FOLDER) if f.startswith("tmdb_movies_") and f.endswith(".json")]
    if last_file_index <= len(all_files):  # Condição ajustada para permitir o acesso ao arquivo correto
        file_name = str("tmdb_movies_" + str(last_file_index) + ".json")
        return os.path.join(TMDB_MOVIES_FOLDER, file_name) 
    return None

# Função para continuar de onde parou
def continue_omdb_search():
    checkpoint = load_checkpoint()
    last_imdb_id = checkpoint["last_imdb_id"]
    last_file_index = checkpoint["last_file_index"]
    
    # Encontra o próximo arquivo de filmes TMDB com base no índice salvo
    current_tmdb_file = get_next_tmdb_file(last_file_index)
    if not current_tmdb_file:
        logging.warning("Nenhum arquivo TMDB encontrado. Verifique se a coleta TMDB foi realizada corretamente.")
        return

    logging.info(f"Arquivo atual: {current_tmdb_file}")

    while current_tmdb_file:
        tmdb_movies = load_tmdb_movies(current_tmdb_file)
        if not tmdb_movies:
            logging.warning(f"Nenhum filme encontrado no arquivo {current_tmdb_file}.")
            break
        
        # Encontramos o índice do último IMDB ID processado (se houver)
        start_index = 0
        if last_imdb_id:
            try:
                start_index = next(i for i, movie in enumerate(tmdb_movies) if movie['imdb_id'] == last_imdb_id) + 1
            except StopIteration:
                logging.warning(f"IMDB ID {last_imdb_id} não encontrado na lista de filmes. Continuando a partir do início.")
                start_index = 0

        for movie in tmdb_movies[start_index:]:
            imdb_id = movie.get("imdb_id")
            if imdb_id:
                try:
                    # Recupera 'awards' e 'ratings' do OMDB
                    omdb_data = movie_omdb_search(imdb_id)
                    logging.info(f"Dados do OMDB para {imdb_id}: {omdb_data}")

                    # Adiciona ou substitui as informações do OMDB diretamente ao filme
                    if 'awards' in omdb_data:
                        movie['awards'] = omdb_data['awards']
                    if 'ratings' in omdb_data:
                        movie['ratings'] = omdb_data['ratings']
                    
                    # Salva o filme atualizado no arquivo JSON
                    save_json(tmdb_movies, current_tmdb_file)
                    
                    # Atualiza o checkpoint com o último imdb_id e arquivo
                    checkpoint["last_imdb_id"] = imdb_id
                    save_checkpoint(checkpoint)
                except RuntimeError:
                    logging.warning("Limite diário atingido em todas as chaves OMDB, processo interrompido.")
                    return  # Não passa para o próximo arquivo, sai do loop atual

        # Passa para o próximo arquivo se o arquivo atual foi completamente processado
        last_file_index += 1
        checkpoint["last_file_index"] = last_file_index
        save_checkpoint(checkpoint)
        
        # Encontra o próximo arquivo TMDB
        current_tmdb_file = get_next_tmdb_file(last_file_index)

        # Log para a troca de arquivo
        logging.info(f"Trocando para o arquivo: {current_tmdb_file}")

# ----------------- EXECUÇÃO -----------------
if __name__ == "__main__":
    logging.info("Iniciando busca de dados no OMDB...")
    continue_omdb_search()
    logging.info("Processo de busca no OMDB finalizado.")
