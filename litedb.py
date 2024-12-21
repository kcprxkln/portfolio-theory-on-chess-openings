import sqlite3
import os
import re

def create_db():
    conn = sqlite3.connect('/home/SQLite/database/chess_games4.db')  # Tworzenie/połączenie z bazą danych
    cursor = conn.cursor()
    
    # Tworzenie tabeli "games"
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS games (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event TEXT,
        site TEXT,
        white TEXT,
        black TEXT,
        result TEXT,
        utc_date TEXT,
        utc_time TEXT,
        white_elo INTEGER,
        black_elo INTEGER,
        white_rating_diff INTEGER,
        black_rating_diff INTEGER,
        eco TEXT,
        opening TEXT,
        time_control TEXT,
        termination TEXT
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

def insert_game_data(metadata, moves):
    conn = sqlite3.connect('/home/SQLite/database/chess_games4.db')  # Połączenie z bazą danych
    cursor = conn.cursor()

    # Funkcja pomocnicza do bezpiecznego parsowania liczby całkowitej
    def safe_int(value, default=0):
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    # Wstawianie danych do tabeli "games"
    cursor.execute('''
    INSERT INTO games (event, site, white, black, result, utc_date, utc_time, white_elo, black_elo,
                       white_rating_diff, black_rating_diff, eco, opening, time_control, termination)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        metadata.get("Event", None),
        metadata.get("Site", None),
        metadata.get("White", None),
        metadata.get("Black", None),
        metadata.get("Result", None),
        metadata.get("UTCDate", None),
        metadata.get("UTCTime", None),
        safe_int(metadata.get("WhiteElo", None)),
        safe_int(metadata.get("BlackElo", None)),
        safe_int(metadata.get("WhiteRatingDiff", None)),
        safe_int(metadata.get("BlackRatingDiff", None)),
        metadata.get("ECO", None),
        metadata.get("Opening", None),
        metadata.get("TimeControl", None),
        metadata.get("Termination", None)
    ))

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

    # Podział na poszczególne gry
    games = re.split(r'\n\n(?=\[Event)', content)  # Rozdzielamy gry po pustych liniach i "[Event"

    for game in games:
        if not game.strip():
            continue  # Pomijamy puste bloki

        # Zaciąganie metadanych z pliku za pomocą wyrażeń regularnych
        metadata = dict(re.findall(r'\[(\w+)\s+"([^"]+)"\]', game))

        # Ekstrakcja ruchów
        moves_section = re.split(r'\n\n', game, maxsplit=1)[-1]  # Część po metadanych
        moves = re.findall(r'\d+\.\s*([a-zA-Z0-9\-\+]+)\s*([a-zA-Z0-9\-\+]+)?', moves_section)
        
        # Przetwarzanie ruchów
        flat_moves = [move for pair in moves for move in pair if move]  # Spłaszczamy listę
        insert_game_data(metadata, flat_moves)

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
folder_path = '/home/SQLite/chess_games'  # Folder z plikami .pgn

# Przetwarzanie wszystkich plików .pgn w folderze
process_all_pub_files(folder_path)
