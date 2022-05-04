from typing import List, Tuple
from contextlib import contextmanager
from psycopg2.extras import execute_values

Poll = Tuple[int, str, str]
Option = Tuple[int, str, int]
Vote = Tuple[str, int]

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

SELECT_POLL = "SELECT * FROM polls WHERE id = %s;"
SELECT_ALL_POLLS = "SELECT * FROM polls;"
SELECT_POLL_OPTIONS = "SELECT * FROM options WHERE poll_id = %s;"
SELECT_LATEST_POLL = """
    WITH latest_id AS (
        SELECT id FROM polls ORDER BY id DESC LIMIT 1
    )

    SELECT * FROM polls WHERE polls.id = (SELECT * FROM latest_id);
"""
SELECT_OPTION = "SELECT * FROM options WHERE id = %s;"
SELECT_VOTES_FOR_OPTION = "SELECT * FROM votes WHERE option_id = %s;"

INSERT_POLL_RETURN_ID = "INSERT INTO polls (title, owner_username) VALUES (%s, %s) RETURNING id;"
INSERT_OPTION_RETURN_ID = "INSERT INTO options (option_text, poll_id) VALUES (%s, %s) RETURNING id;"
INSERT_VOTE = "INSERT INTO votes (username, option_id) VALUES (%s, %s);"


@contextmanager
def get_cursor(connection):
    with connection:
        with connection.cursor() as cursor:
            yield cursor


def create_tables(conn):
    with get_cursor(conn) as cur:
        cur.execute(CREATE_POLLS)
        cur.execute(CREATE_OPTIONS)
        cur.execute(CREATE_VOTES)


def create_poll(conn, title: str, owner: str):
    with get_cursor(conn) as cur:
        cur.execute(INSERT_POLL_RETURN_ID, (title, owner))
        poll_id = cur.fetchone()[0]
        return poll_id


def get_polls(conn) -> List[Poll]:
    with get_cursor(conn) as cur:
        cur.execute(SELECT_ALL_POLLS)
        return cur.fetchall()


def get_poll(conn, poll_id: int) -> Poll:
    with get_cursor(conn) as cur:
        cur.execute(SELECT_POLL, (poll_id,))
        return cur.fetchone()


def get_latest_poll(conn) -> Poll:
    with get_cursor(conn) as cur:
        cur.execute(SELECT_LATEST_POLL)
        return cur.fetchone()


def get_poll_options(conn, poll_id: int) -> List[Option]:
    with get_cursor(conn) as cur:
        cur.execute(SELECT_POLL_OPTIONS, (poll_id,))
        return cur.fetchall()


def get_option(conn, option_id: int) -> Option:
    with get_cursor(conn) as cur:
        cur.execute(SELECT_OPTION, (option_id,))
        return cur.fetchone()


def add_option(conn, option_text: str, poll_id: int):
    with get_cursor(conn) as cur:
        cur.execute(INSERT_OPTION_RETURN_ID, (option_text, poll_id))
        option_id = cur.fetchone()[0]
        return option_id


def get_votes_for_option(conn, option_id: int) -> List[Vote]:
    with get_cursor(conn) as cur:
        cur.execute(SELECT_VOTES_FOR_OPTION, (option_id,))
        return cur.fetchall()


def add_poll_vote(conn, username: str, option_id: int):
    with get_cursor(conn) as cur:
        cur.execute(INSERT_VOTE, (username, option_id))