import psycopg2
import os
import sys
import config

connection = psycopg2.connect(
    database=config.DB_NAME,
    user=config.DB_USER,
    password=config.DB_PASS,
    host=config.DB_IP,
    port=5432,
)

cursor = connection.cursor()


def check_leetcode_user(leetcode_username):
    cursor.execute(
        "SELECT * from account_owner WHERE leetcode_username = %s;",
        (leetcode_username,),
    )
    record = cursor.fetchall()

    # may return empty list
    return record


def check_discord_user(discord_id):
    cursor.execute(
        f"SELECT * from account_owner WHERE discord_username = '{discord_id}';"
    )
    record = cursor.fetchall()
    # may return empty list
    return record


def get_leetcode_from_discord(discord_username):
    cursor.execute(
        f"SELECT leetcode_username from account_owner WHERE discord_username = '{discord_username}';"
    )
    record = cursor.fetchall()
    if len(record) == 0:
        return False
    return record[0][0]


def get_discord_from_leetcode(leetcode_username):
    cursor.execute(
        f"SELECT discord_username from account_owner WHERE leetcode_username = '{leetcode_username}';"
    )
    record = cursor.fetchall()
    if len(record) == 0:
        return False
    return record[0][0]


def add_user(discord_id, leetcode_username):
    try:
        cursor.execute(
            "INSERT INTO account_owner (discord_username, leetcode_username) VALUES (%s, %s);",
            (discord_id, leetcode_username),
        )
        cursor.execute(
            "INSERT INTO users (username) VALUES (%s);", (leetcode_username,)
        )
        # get user id from users table
        cursor.execute(
            "SELECT id from users WHERE username = %s;", (leetcode_username,)
        )
        user_id = cursor.fetchall()[0][0]
        # add user_id to points with 0 points
        cursor.execute(
            "INSERT INTO points (user_id, points) VALUES (%s, 0);", (user_id,)
        )
        connection.commit()
        return (True, "")
    except Exception as e:
        print(e)
        return (False, e)


def remove_user(discord_id):
    try:
        # get leetcode username
        cursor.execute(
            "SELECT leetcode_username FROM account_owner WHERE discord_username = %s;",
            (discord_id,),
        )
        leetcode_username = cursor.fetchall()[0][0]
        # remove user from last_completed
        cursor.execute(
            "DELETE FROM last_completed WHERE user_id = (SELECT id FROM users WHERE username = %s);",
            (leetcode_username,),
        )
        # remove user from user_submissions
        cursor.execute(
            "DELETE FROM user_submissions WHERE user_id = (SELECT id FROM users WHERE username = %s);",
            (leetcode_username,),
        )
        # remove user from points
        cursor.execute(
            "DELETE FROM points WHERE user_id = (SELECT id FROM users WHERE username = %s);",
            (leetcode_username,),
        )
        # remove user from users
        cursor.execute(
            "DELETE FROM users WHERE username = (%s);",
            (leetcode_username,),
        )
        # remove user from account_owner
        cursor.execute(
            "DELETE FROM account_owner WHERE discord_username = %s;",
            (discord_id,),
        )

        connection.commit()
        return (True, "")
    except Exception as e:
        print(e)
        return (False, e)


def add_points(discord_user, leetcode_user, points):
    try:
        if discord_user:
            result = get_leetcode_from_discord(discord_user)
            if result:
                leetcode_username = result
        else:
            leetcode_username = leetcode_user
        statement = f"UPDATE points SET points = points + {points} WHERE user_id = (SELECT id FROM users WHERE username = '{leetcode_username}');"
        cursor.execute(
            statement,
        )
        connection.commit()
        return (True, "")
    except Exception as e:
        print(e)
        return (False, e)


def CLEAR_ALL_POINTS(reset_interval=None, wins=None):
    try:
        cursor.execute("SELECT id FROM points;")
        rows = cursor.fetchall()
        for row in rows:
            if wins:
                cursor.execute(f"UPDATE points SET wins = 0 WHERE id = {row[0]};")
            cursor.execute(f"UPDATE points SET points = 0 WHERE id = {row[0]};")
        cursor.execute("DELETE FROM reset;")
        cursor.execute("INSERT INTO reset (last_reset) VALUES (NOW());")
        if reset_interval is not None:
            cursor.execute(f"UPDATE reset SET reset_interval = {reset_interval};")

        cursor.execute("UPDATE reset SET last_reset = NOW();")
        connection.commit()
    except Exception as e:
        print(e)
        return False
    return True


def add_admin(discord_id):
    try:
        cursor.execute(
            "INSERT INTO admins discord_id VALUES (%s);",
            (discord_id),
        )
        connection.commit()
        return True
    except Exception as e:
        print(e)
        return False
