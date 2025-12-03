import requests
import json
import logging
import os

# ----------------- CONFIGURAÇÃO -----------------
TMDB_API_KEY = "d8eb59f8bd90d3c6a3e0fb649de28dbb"
LANG = "pt-BR"
BASE_URL = "https://api.themoviedb.org/3"
MIN_VOTES = 30

# Caminhos para arquivos
EXTRACTION_FOLDER = "extraction"
LOGS_FOLDER = "logs"

# Arquivo de log
LOG_FILE = os.path.join(LOGS_FOLDER, "tmdb_requests.log")

# Definir o tamanho máximo por arquivo em bytes (20MB)
MAX_FILE_SIZE = 20 * 1024 * 1024  # 20MB

# Contador para arquivos JSON
json_file_counter = 1

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
    logging.info(f"Dados salvos em {file_path}")

def get_next_json_file():
    """Gera o próximo arquivo JSON baseado no contador"""
    global json_file_counter
    file_path = os.path.join(EXTRACTION_FOLDER, f"tmdb_movies_{json_file_counter}.json")
    json_file_counter += 1
    return file_path

def is_file_too_large(file_path):
    """Verifica se o arquivo excede o limite de tamanho"""
    return os.path.exists(file_path) and os.path.getsize(file_path) > MAX_FILE_SIZE

# ----------------- TMDB API -----------------
def movie_discovery_search(page=1, year=None):
    """Busca filmes no TMDB com base no ano e página"""
    params = {
        "api_key": TMDB_API_KEY,
        "language": LANG,
        "sort_by": "popularity.desc",
        "page": page,
        "primary_release_year": year,
        "vote_count.gte": MIN_VOTES,
        "include_adult": False,
    }
    url = f"{BASE_URL}/discover/movie"
    try:
        res = requests.get(url, params=params, timeout=10)
        res.raise_for_status()
        data = res.json()
        return data.get("results", []), data.get("total_pages", 0)
    except requests.RequestException as e:
        logging.warning(f"Erro na requisição Discover: {e}")
        return [], 0

def movie_details_search(movie_id):
    """Recupera detalhes de um filme pelo ID"""
    url = f"{BASE_URL}/movie/{movie_id}?api_key={TMDB_API_KEY}&language={LANG}"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        return res.json()
    except requests.RequestException:
        return None

def movie_credits_search(movie_id):
    """Recupera informações sobre o elenco e equipe de um filme"""
    url = f"{BASE_URL}/movie/{movie_id}/credits?api_key={TMDB_API_KEY}&language={LANG}"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()
        cast = [{"name": c.get("name"), "character": c.get("character")} for c in data.get("cast", [])]
        crew = [{"name": c.get("name"), "department": c.get("department")} for c in data.get("crew", [])]
        return {"cast": cast, "crew": crew}
    except requests.RequestException:
        return {"cast": [], "crew": []}

def movie_keywords_search(movie_id):
    """Recupera as palavras-chave associadas a um filme"""
    url = f"{BASE_URL}/movie/{movie_id}/keywords?api_key={TMDB_API_KEY}"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()
        return [k.get("name") for k in data.get("keywords", [])]
    except requests.RequestException:
        return []

# Função para coletar dados de um filme completo
def collect_movie_data(movie):
    """Coleta todos os dados de um filme usando TMDB"""
    movie_id = movie["id"]
    details = movie_details_search(movie_id)
    if not details:
        return None

    credits = movie_credits_search(movie_id)
    keywords = movie_keywords_search(movie_id)

    movie_data = {
        "id": movie_id,
        "title": details.get("title"),
        "original_title": details.get("original_title"),
        "overview": details.get("overview"),
        "release_date": details.get("release_date"),
        "runtime": details.get("runtime"),
        "budget": details.get("budget"),
        "revenue": details.get("revenue"),
        "genres": [g["name"] for g in details.get("genres", [])],
        "keywords": keywords,
        "production_companies": [c["name"] for c in details.get("production_companies", [])],
        "popularity": details.get("popularity"),
        "vote_average": details.get("vote_average"),
        "vote_count": details.get("vote_count"),
        "original_language": details.get("original_language"),
        "adult": details.get("adult"),
        "poster_path": details.get("poster_path"),
        "backdrop_path": details.get("backdrop_path"),
        "imdb_id": details.get("imdb_id"),
        "belongs_to_collection": details.get("belongs_to_collection"),
        "credits": credits
    }

    return movie_data

# Função principal para realizar a coleta
def movie_extraction():
    """Função principal de extração de filmes"""
    json_file = []
    existing_ids = set()
    years = list(range(2025, 1969, -1))  # Anos de 2025 a 1969

    for year in years:
        logging.info(f"Iniciando coleta para o ano {year}...")
        page = 1
        while True:
            movies, total_pages = movie_discovery_search(page=page, year=year)
            if not movies:
                break

            new_results = 0
            for movie in movies:
                if movie["id"] in existing_ids:
                    continue

                # Coleta dados do filme
                movie_data = collect_movie_data(movie)
                if movie_data:
                    json_file.append(movie_data)
                    existing_ids.add(movie_data["id"])
                    new_results += 1
                    logging.info(f"{movie_data['title']} ({movie_data['release_date']}) adicionado.")

            page += 1
            if page > total_pages:
                break

        if new_results > 0:
            # Salva os dados em arquivos separados se o tamanho for excedido
            current_json_file = get_next_json_file()
            if is_file_too_large(current_json_file):
                logging.info(f"O arquivo {current_json_file} é grande demais, criando um novo arquivo.")
                current_json_file = get_next_json_file()

            save_json(json_file, current_json_file)
            json_file = []  # Limpa para novos dados
            logging.info(f"{new_results} novos filmes salvos.\n")

# ----------------- EXECUÇÃO -----------------
if __name__ == "__main__":
    logging.info("Iniciando a coleta de filmes do TMDB...")
    movie_extraction()
    logging.info("Coleta de filmes do TMDB finalizada.")
