import time
import psycopg2
import requests
from datetime import datetime
import config


# db connection setup
def get_db_connection():
    conn = psycopg2.connect(
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASS,
        host=config.DB_IP,
    )
    conn.autocommit = True
    return conn


def initialize_users(conn):
    cur = conn.cursor()
    cur.execute("SELECT id FROM users")
    rows = cur.fetchall()
    for row in rows:
        cur.execute("SELECT * FROM last_completed WHERE user_id = %s", (row[0],))
        if not cur.fetchone():
            cur.execute(
                "INSERT INTO last_completed (user_id, problem_name,completed_at) VALUES (%s, %s, %s)",
                (row[0], "", datetime.now()),
            )
    conn.commit()
    cur.close()


def award_points(conn):
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT id FROM users")
    rows = cur.fetchall()
    for row in rows:
        cur.execute("SELECT username FROM users WHERE id = %s", (row[0],))
        leetcode_username = cur.fetchone()[0]
        url = "http://localhost:5000/api/leetcode_ac"
        headers = {"leetcode-username": leetcode_username}
        response = requests.get(url, headers=headers)
        json = response.json()
        index = json["count"] - 1
        submissions = json["submission"]
        while index >= 0:
            # get problem name and time
            problem_name = submissions[index]["title"]
            completed_at = submissions[index]["timestamp"]
            completed_at = datetime.fromtimestamp(int(completed_at))
            # check if problem was alr done
            cur.execute(
                "SELECT * FROM user_submissions WHERE user_id = %s AND problem_name = %s",
                (row[0], problem_name),
            )
            if not cur.fetchone():
                # print problem name
                # print(problem_name)
                # if problem wasnt alr done insert it into table
                cur.execute(
                    "INSERT INTO user_submissions (user_id, problem_name, completed_at) VALUES (%s, %s, %s)",
                    (row[0], problem_name, completed_at),
                )
                # update last_completed table with latest submission
                cur.execute(
                    "UPDATE last_completed SET problem_name = %s, completed_at = %s WHERE user_id = %s",
                    (problem_name, completed_at, row[0]),
                )
                # award 3 points to user
                cur.execute(
                    "UPDATE points SET points = points + 3 WHERE user_id = %s",
                    (row[0],),
                )

            index -= 1


if __name__ == "__main__":
    conn = get_db_connection()
    while True:
        initialize_users(conn)
        award_points(conn)
        # delay for 1 minute
        time.sleep(60)
