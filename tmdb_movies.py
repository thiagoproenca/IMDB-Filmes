import requests
import json
import os
import time
import logging
from datetime import datetime

# ----------------- CONFIGURAÇÃO -----------------
TMDB_API_KEY = "d8eb59f8bd90d3c6a3e0fb649de28dbb"
OMDB_API_KEYS = ["58003cc5", "dabeb3cd", "9d6e27a9"]
omdb_key_index = 0  # índice da chave OMDB atual

JSON_FILE = "movies.json"
CHECKPOINT_FILE = "checkpoint.json"
LANG = "pt-BR"
BASE_URL = "https://api.themoviedb.org/3"
MIN_VOTES = 30

# Logging
logging.basicConfig(
    filename="movies_extraction.log",
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
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_json(data, file_path):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_checkpoint():
    return load_json(CHECKPOINT_FILE) or {"year_index": 0, "page": 1}

def save_checkpoint(checkpoint):
    save_json(checkpoint, CHECKPOINT_FILE)

# ----------------- TMDB -----------------
def movie_discovery_search(page=1, year=None):
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
    url = f"{BASE_URL}/movie/{movie_id}?api_key={TMDB_API_KEY}&language={LANG}"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        return res.json()
    except requests.RequestException:
        return None

def movie_credits_search(movie_id):
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
    url = f"{BASE_URL}/movie/{movie_id}/keywords?api_key={TMDB_API_KEY}"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()
        return [k.get("name") for k in data.get("keywords", [])]
    except requests.RequestException:
        return []

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
            if data.get("Error") == "Request limit reached!":
                logging.warning(f"Limite diário atingido para a chave OMDB {key}, trocando de chave...")
                omdb_key_index += 1
                continue  # tenta próxima chave
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
            logging.warning(f"Erro na requisição OMDB com chave {key}, tentando próxima...")
            omdb_key_index += 1
    
    # todas as chaves esgotadas
    raise RuntimeError("Todas as chaves OMDB atingiram o limite diário, parada da coleta.")

# ----------------- EXTRAÇÃO -----------------
def movie_extraction():
    json_file = load_json(JSON_FILE)
    existing_ids = {f["id"] for f in json_file}
    checkpoint = load_checkpoint()
    logging.info(f"Iniciando coleta... ({len(json_file)} filmes já salvos)")

    years = list(range(2025, 1969, -1))
    try:
        for year_index in range(checkpoint["year_index"], len(years)):
            year = years[year_index]
            logging.info(f"\n[COMEÇANDO COLETA DE {year}]\n")

            page = checkpoint["page"]
            total_pages = 1
            while page <= total_pages:
                movies, total_pages = movie_discovery_search(page, year)
                if not movies:
                    break

                new_results = 0
                for item in movies:
                    movie_id = item["id"]
                    if movie_id in existing_ids:
                        continue

                    details = movie_details_search(movie_id)
                    if not details:
                        continue

                    credits = movie_credits_search(movie_id)
                    keywords = movie_keywords_search(movie_id)
                    try:
                        omdb_data = movie_omdb_search(details.get("imdb_id"))
                    except RuntimeError:
                        # Limite do OMDB atingido em todas as chaves
                        checkpoint["year_index"] = year_index
                        checkpoint["page"] = page
                        save_checkpoint(checkpoint)
                        logging.warning("Parada da coleta: todas as chaves OMDB atingiram o limite diário.")
                        return

                    movie = {
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
                        "omdb_awards": omdb_data.get("awards"),
                        "omdb_ratings": omdb_data.get("ratings"),
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

                    json_file.append(movie)
                    existing_ids.add(movie_id)
                    new_results += 1

                    logging.info(f"{movie['title']} ({movie['release_date']}) adicionado. Total: {len(json_file)}")
                    time.sleep(0.35)

                if new_results > 0:
                    save_json(json_file, JSON_FILE)
                    logging.info(f"{new_results} novos filmes salvos.\n")

                page += 1
                checkpoint["page"] = page
                save_checkpoint(checkpoint)
                time.sleep(2.5)

            checkpoint["year_index"] = year_index + 1
            checkpoint["page"] = 1
            save_checkpoint(checkpoint)

    except KeyboardInterrupt:
        logging.info("\nColeta interrompida manualmente.")
        save_json(json_file, JSON_FILE)
        checkpoint["year_index"] = year_index
        checkpoint["page"] = page
        save_checkpoint(checkpoint)
        logging.info(f"Total final: {len(json_file)} filmes salvos.")

# ----------------- EXECUÇÃO -----------------
if __name__ == "__main__":
    movie_extraction()