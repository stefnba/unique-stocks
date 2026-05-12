import requests
from prefect import flow, task


@task
def fetch(number):
    pokemon_url = f"https://pokeapi.co/api/v2/pokemon/{number}"

    response = requests.get(pokemon_url)
    return response.text


@task
def fetch_all():

    return [fetch.fn(url) for url in range(1, 500)]


@task
def process_result(result):
    print(result[:100])  # Print the first 100 characters of each result for brevity


@flow(log_prints=True)
def sync_test() -> None:

    results = fetch_all()
    for result in results:
        process_result(result)


if __name__ == "__main__":

    sync_test()
