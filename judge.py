import time
import traceback
import psycopg2
import requests
from datetime import date
from datetime import datetime
import config
import dbfuncs


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


def get_last_reset(conn):
    cur = conn.cursor()
    cur.execute("SELECT last_reset FROM reset")
    last_reset = cur.fetchone()[0]
    cur.close()
    return last_reset

def get_points(conn,problem_slug):
    cur = conn.cursor()
    query = f"SELECT points FROM difficulty WHERE titleslug = '{problem_slug}';"
    cur.execute(query)
    result = cur.fetchall()
    if len(result) > 0:
        return result[0][0]
    else:
        return -1


def get_reset_interval(conn):
    cur = conn.cursor()
    cur.execute("SELECT reset_interval FROM reset")
    reset_interval = cur.fetchone()[0]
    cur.close()
    return reset_interval


def clear_and_award_win(conn):
    # turn last_reset into a datetime
    last_reset = datetime.combine(get_last_reset(conn), datetime.min.time())
    reset_interval = get_reset_interval(conn)
    print("Last reset: ", last_reset)
    print("Reset interval: ", reset_interval)
    # get fractional days since last reset
    x = datetime.now() - last_reset
    time_since_reset = (x.days * 86400 + x.seconds) / 86400
    print("Time since last reset: ", time_since_reset)
    if time_since_reset >= reset_interval:
        print("Awarding a win and resetting points")
        # get user with most points and increment their wins column
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM points ORDER BY points DESC LIMIT 1")
        user_id = cur.fetchone()[0]
        # hopefully fix null
        cur.execute("UPDATE points SET wins = COALESCE(wins, 0) + 1 WHERE user_id = %s", (user_id,))
        # insert 2 columsn into win_history table, first is user_id second is timestamp of now
        timestamp_now = datetime.now()
        cur.execute(
            "SELECT 1 FROM win_history WHERE user_id = %s AND timestamp::date = %s::date",
            (user_id, timestamp_now)
        )
        if cur.fetchone() is None:
            # Insert new win record if it doesn't already exist
            cur.execute("INSERT INTO win_history (user_id, timestamp) VALUES (%s, %s)", (user_id, timestamp_now))
        else:
            print("Identical row already exists in win_history; skipping insertion.")
        conn.commit()
        cur.close()

        # clear all points
        dbfuncs.CLEAR_ALL_POINTS(reset_interval)


# determine the difficulty and points of the given problem
def check_problem_worth(conn,problem_slug):
    points = get_points(conn,problem_slug)
    # check if the problem slug is already in the difficulty table
    if points != -1:
        print("Problem already in difficulty table")
        return points
    print("Problem not in difficulty table")
    # go to the problem page and get the difficulty
    url = f"http://localhost:3000/select?titleSlug={problem_slug}"
    response = requests.get(url)
    difficulty = ''
    valid = True
    if response:
        difficulty = response.json()["difficulty"].lower()
    if difficulty == "easy":
        points = 1
    elif difficulty == "medium":
        points = 3
    elif difficulty == "hard":
        points = 10
    else:
        valid = False
        print("Invalid difficulty")
        points = 1
    # insert into table
    cur = conn.cursor()
    if valid:
        
        cur.execute("INSERT INTO difficulty (titleslug,points) VALUES (%s,%s)",(problem_slug,points))
        conn.commit()
    cur.close()
    return points
    
    
    

# this is the most dogshit function ever, fix it later (this shit is not getting fixed)
def award_points(conn):
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT id FROM users")
    rows = cur.fetchall()
    cur.execute("SELECT last_reset FROM reset")
    last_reset = cur.fetchall()[0][0]
    last_reset = datetime.combine(last_reset, datetime.min.time())

    for row in rows:
        user_id = row[0]

        # Get LeetCode username
        cur.execute("SELECT username FROM users WHERE id = %s", (user_id,))
        leetcode_username = cur.fetchone()[0]
        print(f"Processing user: {leetcode_username}")

        # Get user's last completed submission
        cur.execute(
            "SELECT problem_name, completed_at FROM last_completed WHERE user_id = %s",
            (user_id,),
        )
        last_completed = cur.fetchone()
        last_completed_problem = last_completed[0]

        # Flag to track if last_completed needs updating due to being uninitialized
        needs_initial_update = False

        # Check if last_completed is empty (i.e., first valid submission needs to update last_completed)
        if last_completed_problem == "":
            needs_initial_update = True
            print(
                f"User {leetcode_username} has no previous completed problems. Initializing first valid submission."
            )

        url = "https://server.rakibshahid.com/api/leetcode_ac"
        headers = {"leetcode-username": leetcode_username}
        response = requests.get(url, headers=headers)

        if response:
            json = response.json()
            index = json["count"] - 1
            submissions = json["submission"]

            while index >= 0:
                # Get problem name and submission timestamp
                problem_name = submissions[index]["title"]
                problem_slug = submissions[index]["titleSlug"]
                problem_points = check_problem_worth(conn,problem_slug)
                completed_at = submissions[index]["timestamp"]
                completed_at = datetime.fromtimestamp(int(completed_at))

                # get latest each time
                cur.execute(
                    "SELECT problem_name, completed_at FROM last_completed WHERE user_id = %s",
                    (user_id,),
                )
                last_completed = cur.fetchone()
                last_completed_problem = last_completed[0]

                print(
                    f"Checking submission: {problem_name} completed at {completed_at}"
                )

                # If last_completed is empty, automatically use this first valid submission
                if needs_initial_update:
                    print(f"Initializing last completed problem with: {problem_name}")
                    # Update the last_completed table with the first problem
                    cur.execute(
                        "UPDATE last_completed SET problem_name = %s, completed_at = %s WHERE user_id = %s",
                        (problem_name, completed_at, user_id),
                    )
                    needs_initial_update = False

                # For subsequent submissions, only award points if they are after the last completed time
                elif completed_at > last_reset:
                    # Check if the problem was already completed
                    cur.execute(
                        "SELECT * FROM user_submissions WHERE user_id = %s AND problem_name = %s",
                        (user_id, problem_name),
                    )
                    if not cur.fetchone():
                        print(f"Awarding points for problem: {problem_name}")

                        # Insert new submission into user_submissions table
                        cur.execute(
                            "INSERT INTO user_submissions (user_id, problem_name, completed_at) VALUES (%s, %s, %s)",
                            (user_id, problem_name, completed_at),
                        )
                        # Update last_completed table with the latest submission
                        cur.execute(
                            "UPDATE last_completed SET problem_name = %s, completed_at = %s WHERE user_id = %s",
                            (problem_name, completed_at, user_id),
                        )
                        # Award 3 points to the user
                        cur.execute(
                            "UPDATE points SET points = points + %s WHERE user_id = %s",
                            (problem_points,user_id,),
                        )
                else:
                    print(
                        f"Skipping problem: {problem_name}, completed before or on the last reset of {last_reset}"
                    )
                
                index -= 1
            time.sleep(2)
    cur.close()


if __name__ == "__main__":

    conn = get_db_connection()

    # create example of jan 1 2023
    # example = datetime.date(2023, 1, 1)
    # print(example > last_reset)
    while True:
        try:
            initialize_users(conn)
            clear_and_award_win(conn)
            award_points(conn)
        except Exception as e:
           traceback.print_exc()
        # delay for 10 minutes
        time.sleep(600)
