import requests
import json

API_KEY = "d8eb59f8bd90d3c6a3e0fb649de28dbb"
LINGUA = "pt-BR"
URL = f"https://api.themoviedb.org/3/genre/movie/list?api_key={API_KEY}&language={LINGUA}"

res = requests.get(URL)

if res.status_code == 200:
    data = res.json()["genres"]
    generos = {g["id"]: g["name"] for g in data}
    
    with open("generos.json", "w", encoding="utf-8") as f:
        json.dump(generos, f, ensure_ascii=False, indent=2)
    
    print("Arquivo 'generos.json' criado com sucesso!")
else:
    print(f"Erro ao buscar gêneros: {res.status_code}")
