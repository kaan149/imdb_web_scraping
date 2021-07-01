import requests
import psycopg2
import numpy as np
import re
import sys
import time
import json
from bs4 import BeautifulSoup


class Movie:
    def __init__(self, name, release_year, imdb_score):
        self.name = name
        self.release_year = release_year
        self.imdb_score = imdb_score

def main():
    print("-----  PROGRAM  -----")
    print("write 100 for top 100 movies\nwrite 250 for top 250 movies\nwrite 1000 for top 1000 movies\nwrite q for exit")
    value = str(input("input : "))

    if((value == "100") | (value == "250") | (value == "1000")):
        fetch_and_save(value)
    elif(value == "q"):
        print("program is terminated")
        sys.exit(0)
    else:
        print("invalid input")
        time.sleep(1)
        main()


def fetch_and_save(input):    
    # we define an array to scroll through the pages. One page contains 50 movies in imdb.
    pages = np.arange(1, int(input) + 1, 50)

    # we browse page by page and send movies to the movies list. Then we send this list to postgre database
    movies = []
    for page in pages:

        # we add input to the website link in order to decide how many movies are wanted by user
        website = requests.get("https://www.imdb.com/search/title/?groups=top_" + input + "&start=" + str(page) + "&ref_=adv_nxt")    
        soup = BeautifulSoup(website.text, "html.parser")
        list = soup.find("div", {"class": "lister-list"}
                        ).find_all("div", {"class": "lister-item mode-advanced"})

        for movie in list:
            name = movie.find("div", {"class": "lister-item-content"}).h3.a.text
            release_year = re.sub('[^0-9,.]', '', movie.find("div", {"class": "lister-item-content"}).h3.find(
                "span", {"class": "lister-item-year text-muted unbold"}).text)
            imdb_score = movie.find("div", {"class": "lister-item-content"}).div.find(
                "div", {"class": "inline-block ratings-imdb-rating"}).strong.text

            # we create a movie object for proper layout
            inner_movie = Movie(name, release_year, imdb_score)
            movies.append(inner_movie)

    connect_and_save(movies)


def connect_and_save(movies):
    print("connect and save function")
    try:
        connection = psycopg2.connect(user="postgres",
                                    password="12345",
                                    host="localhost",
                                    port="5432",
                                    database="movies")
        cursor = connection.cursor()

        postgres_insert_query = """INSERT INTO movies (name, release_year, imdb_score) VALUES (%s,%s,%s)"""
        for movie in movies:
            record_to_insert = (movie.name, movie.release_year, movie.imdb_score)
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into movies table")

        # json file saving
        json_string = json.dumps([movie.__dict__ for movie in movies])
        with open('movies.json', 'w') as file:
            json.dump(json_string, file)
        

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into movies table", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

if __name__ == '__main__':
    main()