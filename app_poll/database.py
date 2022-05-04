from typing import List, Tuple
from psycopg2.extras import execute_values

Poll = Tuple[int, str, str]
Vote = Tuple[str, int]
PollWithOption = Tuple[int, str, str, int, str, int]
PollResults = Tuple[int, str, int, float]

CREATE_POLLS = """
    CREATE TABLE IF NOT EXISTS polls (
        id SERIAL PRIMARY KEY,
        title TEXT,
        owner_username TEXT
    );
"""
CREATE_OPTIONS = """
    CREATE TABLE IF NOT EXISTS options (
        id SERIAL PRIMARY KEY,
        option_text TEXT,
        poll_id INTEGER,
        FOREIGN KEY(poll_id) REFERENCES polls(id)
    );
"""
CREATE_VOTES = """
    CREATE TABLE IF NOT EXISTS votes (
        username TEXT,
        option_id INTEGER,
        FOREIGN KEY(option_id) REFERENCES options(id)
    );
"""

SELECT_ALL_POLLS = "SELECT * FROM polls;"
SELECT_POLL_WITH_OPTIONS = """
    SELECT * FROM polls
    JOIN options ON polls.id = options.poll_id
    WHERE polls.id = %s;
"""
SELECT_LATEST_POLL = """
    WITH latest_id AS (
        SELECT id FROM polls ORDER BY id DESC LIMIT 1
    )

    SELECT * FROM polls
    JOIN options ON polls.id = options.poll_id
    WHERE polls.id = (SELECT * FROM latest_id);
"""
SELECT_RANDOM_VOTE = "SELECT * FROM votes WHERE option_id = %s ORDER BY RANDOM() LIMIT 1;"
SELECT_POLL_VOTE_DETAILS = """
SELECT
    options.id,
    options.option_text,
    COUNT(votes.option_id) AS votes_total,
    COUNT(votes.option_id) / SUM(COUNT(votes.option_id)) OVER() * 100 AS votes_perc
FROM options
LEFT JOIN votes ON options.id = votes.option_id 
WHERE options.poll_id = %s
GROUP BY options.id;
"""

INSERT_POLL_RETURN_ID = "INSERT INTO polls (title, owner_username) VALUES (%s, %s) RETURNING id;"
INSERT_OPTION = "INSERT INTO options (option_text, poll_id) VALUES %s;"
INSERT_VOTE = "INSERT INTO votes (username, option_id) VALUES (%s, %s);"

def create_tables(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_POLLS)
            cur.execute(CREATE_OPTIONS)
            cur.execute(CREATE_VOTES)

def get_polls(conn) -> List[Poll]:
    with conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_ALL_POLLS)
            return cur.fetchall()

def get_latest_poll(conn) -> List[PollWithOption]:
    with conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_LATEST_POLL)
            return cur.fetchall()

def get_poll_details(conn, poll_id: int) -> List[PollWithOption]:
    with conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_POLL_WITH_OPTIONS, (poll_id,))
            return cur.fetchall()

def get_poll_and_vote_results(conn, poll_id: int) -> List[PollResults]:
    with conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_POLL_VOTE_DETAILS, (poll_id,))
            return cur.fetchall()

def get_random_poll_vote(conn, option_id: int) -> Vote:
    with conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_RANDOM_VOTE, (option_id,))
            return cur.fetchone()

def create_poll(conn, title: str, owner: str, options: List[str]):
    with conn:
        with conn.cursor() as cur:
            cur.execute(INSERT_POLL_RETURN_ID, (title, owner))
            poll_id = cur.fetchone()[0]
            option_values = [(op_text, poll_id) for op_text in options]
            execute_values(cur, INSERT_OPTION, option_values)

def add_poll_vote(conn, username: str, option_id: int):
    with conn:
        with conn.cursor() as cur:
            cur.execute(INSERT_VOTE, (username, option_id))