import sqlite3
import os
import re

# Funkcja do tworzenia bazy danych i tabel
def create_db():
    conn = sqlite3.connect('./database/chess_games.db')  # Tworzenie/połączenie z bazą danych
    cursor = conn.cursor()
    
    # Tworzenie tabeli "games"
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS games (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event TEXT,
        site TEXT,
        round TEXT,
        date TEXT,
        white_person TEXT,
        black_person TEXT,
        white_elo INTEGER,
        black_elo INTEGER,
        result TEXT
    )
    ''')

    # Tworzenie tabeli "moves"
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS moves (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER,
        move_number INTEGER,
        white_move TEXT,
        black_move TEXT,
        FOREIGN KEY (game_id) REFERENCES games(id)
    )
    ''')

    conn.commit()  # Zatwierdzenie zmian
    conn.close()  # Zamknięcie połączenia

# Funkcja do wstawiania danych do tabeli "games" i "moves"
def insert_game_data(event, site, round_num, date, white_person, black_person, white_elo, black_elo, result, moves):
    conn = sqlite3.connect('./database/chess_games.db')  # Połączenie z bazą danych
    cursor = conn.cursor()

    # Wstawianie danych do tabeli "games"
    cursor.execute('''
    INSERT INTO games (event, site, round, date, white_person, black_person, white_elo, black_elo, result)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (event, site, round_num, date, white_person, black_person, white_elo, black_elo, result))

    game_id = cursor.lastrowid  # Pobranie ID ostatnio wstawionego rekordu (gra)

    # Wstawianie danych do tabeli "moves" (ruchy)
    move_number = 1
    for i in range(0, len(moves), 2):  # Iteracja przez ruchy (pary białych i czarnych)
        white_move = moves[i]
        black_move = moves[i+1] if i+1 < len(moves) else None
        cursor.execute('''
        INSERT INTO moves (game_id, move_number, white_move, black_move)
        VALUES (?, ?, ?, ?)
        ''', (game_id, move_number, white_move, black_move))
        move_number += 1

    conn.commit()  # Zatwierdzenie zmian
    conn.close()  # Zamknięcie połączenia

# Funkcja do przetwarzania pliku PGN
def process_pub_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    # Zaciąganie metadanych z pliku za pomocą wyrażeń regularnych
    event = re.search(r'\[Event "(.*?)"\]', content).group(1)
    site = re.search(r'\[Site "(.*?)"\]', content).group(1)
    round_num = re.search(r'\[Round "(.*?)"\]', content).group(1)
    date = re.search(r'\[Date "(.*?)"\]', content).group(1)
    white_person = re.search(r'\[White "(.*?)"\]', content).group(1)
    black_person = re.search(r'\[Black "(.*?)"\]', content).group(1)
    white_elo = int(re.search(r'\[WhiteElo "(.*?)"\]', content).group(1))
    black_elo = int(re.search(r'\[BlackElo "(.*?)"\]', content).group(1))
    result = re.search(r'\[Result "(.*?)"\]', content).group(1)

    # Ekstrakcja ruchów z treści pliku
    moves = []
    content_lines = content.splitlines()

    # Usuwamy pierwszą linię (która zawiera datę)
    content_lines = content_lines[1:]

    # Zbieramy wszystkie ruchy w jednej linii
    move_line = ''.join(content_lines).replace("\n", " ")

    # Używamy wyrażenia regularnego do wyłapania numerów ruchów i samych ruchów (biały, czarny)
    # Tylko prawidłowe ruchy będą dopasowane
    move_tokens = re.findall(r'\d+\.\s*([a-zA-Z0-9\-\+]+)\s*([a-zA-Z0-9\-\+]+)', move_line)

    # Przetwarzanie ruchów (pary białego i czarnego)
    move_number = 0  # Zaczynamy numerowanie od 1
    for white_move, black_move in move_tokens:
        moves.append(white_move)
        moves.append(black_move)
        move_number += 1

    # Wstawianie danych do bazy
    insert_game_data(event, site, round_num, date, white_person, black_person, white_elo, black_elo, result, moves)


# Funkcja do przetwarzania wszystkich plików .pgn w folderze
def process_all_pub_files(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith(".pgn"):  # Sprawdzanie pliku .pgn
            file_path = os.path.join(folder_path, filename)
            print(f"Processing file: {file_path}")
            process_pub_file(file_path)

# Tworzenie bazy danych i tabel
create_db()

# Folder, w którym znajdują się pliki .pgn
folder_path = './chess_games'  # Folder z plikami .pgn

# Przetwarzanie wszystkich plików .pgn w folderze
process_all_pub_files(folder_path)
