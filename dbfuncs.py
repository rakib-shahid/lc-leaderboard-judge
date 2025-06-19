import psycopg2
from functools import wraps
import os
import time
import sys
import traceback

sys.path.append(os.path.abspath("../"))
import config


class DBConnection:
    def __init__(self):
        self.connection = psycopg2.connect(
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASS,
            host=config.DB_IP,
            port=5432,
        )
        self.cursor = self.connection.cursor()

    def close(self):
        self.cursor.close()
        self.connection.close()


def with_db(func):
    """Decorator to handle database connection and cursor."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        conn = DBConnection()
        try:
            result = func(conn.cursor, *args, **kwargs)
            conn.connection.commit()
            return result
        except Exception as e:
            print(traceback.format_exc())
            conn.connection.rollback()
            raise e
        finally:
            conn.close()

    return wrapper


def track_queries(func):
    @wraps(func)
    async def wrapper(self, interaction, *args, **kwargs):
        update_query_count(interaction.user.id, interaction.user.name)
        return await func(self, interaction, *args, **kwargs)

    return wrapper


@with_db
def update_query_count(cursor, discord_id: int, discord_user: str):
    try:
        cursor.execute(
            "SELECT id, discord_id, discord_name FROM users WHERE discord_id = %s;",
            (discord_id,),
        )
        result = cursor.fetchone()
        if not result:
            print(f"[TRACKING] No user found for discord_user '{discord_user}'")
            return
        user_id, current_discord_id, current_discord_name = result

        if (
            current_discord_name is None
            or current_discord_name.lower() != discord_user.lower()
        ):
            cursor.execute(
                "UPDATE users SET discord_name = %s WHERE id = %s;",
                (discord_user, user_id),
            )

        cursor.execute("SELECT 1 FROM queries WHERE user_id = %s;", (user_id,))
        if cursor.fetchone():
            cursor.execute(
                "UPDATE queries SET queries = queries + 1 WHERE user_id = %s;",
                (user_id,),
            )
        else:
            cursor.execute(
                "INSERT INTO queries (user_id, discord_id, queries) VALUES (%s, %s, %s);",
                (user_id, discord_id, 1),
            )

    except Exception as e:
        print("[TRACKING]: Error tracking! See error")
        traceback.print_exc()


@with_db
def check_leetcode_user(cursor, leetcode_username):
    cursor.execute(
        "SELECT * FROM users WHERE LOWER(username) = LOWER(%s);",
        (leetcode_username,),
    )
    return cursor.fetchall()


@with_db
def check_discord_user(cursor, discord_username):
    cursor.execute(
        "SELECT * FROM users WHERE LOWER(discord_name) = LOWER(%s);",
        (discord_username,),
    )
    return cursor.fetchall()


@with_db
def get_leetcode_from_discord(cursor, discord_username):
    cursor.execute(
        "SELECT username FROM users WHERE LOWER(discord_name) = LOWER(%s);",
        (discord_username,),
    )
    record = cursor.fetchall()
    return record[0][0] if record else None


@with_db
def get_discord_from_leetcode(cursor, leetcode_username):
    cursor.execute(
        "SELECT discord_name FROM users WHERE LOWER(username) = LOWER(%s);",
        (leetcode_username,),
    )
    record = cursor.fetchall()
    return record[0][0] if record else None


@with_db
def add_user(cursor, discord_username, discord_id, leetcode_username):
    try:
        cursor.execute(
            """
            INSERT INTO users (username, discord_id, discord_name)
            VALUES (LOWER(%s), %s, %s)
            ON CONFLICT (username) DO NOTHING;
            """,
            (leetcode_username, discord_id, discord_username),
        )

        cursor.execute(
            "SELECT id FROM users WHERE LOWER(username) = LOWER(%s);",
            (leetcode_username,),
        )
        user_id = cursor.fetchone()[0]

        cursor.execute(
            "INSERT INTO points (user_id, points, wins) VALUES (%s, 0, 0) ON CONFLICT (user_id) DO NOTHING;",
            (user_id,),
        )

        cursor.execute(
            "INSERT INTO queries (user_id, discord_id, queries) VALUES (%s, %s, 1) ON CONFLICT (user_id) DO NOTHING;",
            (user_id, discord_id),
        )

        cursor.execute(
            """
            INSERT INTO last_completed (user_id, problem_name, completed_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) DO NOTHING;
            """,
            (user_id, "PLACEHOLDERPROBLEMNAMEFORINITIALREGISTRATION"),
        )

        return True, ""
    except Exception as e:
        return False, str(e)


@with_db
def remove_user(cursor, discord_id):
    try:
        cursor.execute("SELECT id FROM users WHERE discord_id = %s;", (discord_id,))
        result = cursor.fetchone()
        if not result:
            return False, f"No user found with discord_id {discord_id}"

        user_id = result[0]

        cursor.execute("DELETE FROM last_completed WHERE user_id = %s;", (user_id,))
        cursor.execute("DELETE FROM user_submissions WHERE user_id = %s;", (user_id,))
        cursor.execute("DELETE FROM points WHERE user_id = %s;", (user_id,))
        cursor.execute("DELETE FROM queries WHERE user_id = %s;", (user_id,))
        cursor.execute("DELETE FROM users WHERE id = %s;", (user_id,))

        return True, ""
    except Exception as e:
        return False, str(e)


@with_db
def add_points(cursor, discord_user, leetcode_user, points):
    try:
        leetcode_username = (
            get_leetcode_from_discord(discord_user) if discord_user else leetcode_user
        )
        cursor.execute(
            "UPDATE points SET points = points + %s WHERE user_id = (SELECT id FROM users WHERE LOWER(username) = LOWER(%s));",
            (points, leetcode_username),
        )
        return True, ""
    except Exception as e:
        return False, str(e)


@with_db
def get_user_points(cursor, discord_user):
    try:
        leetcode_username = get_leetcode_from_discord(discord_user)
        cursor.execute(
            "SELECT points FROM points WHERE user_id = (SELECT id FROM users WHERE LOWER(username) = LOWER(%s));",
            (leetcode_username,),
        )
        return cursor.fetchone()[0]
    except Exception as e:
        return None


@with_db
def get_win_history(cursor, original_rows=False):
    cursor.execute(
        "SELECT username, timestamp FROM win_history JOIN users ON win_history.user_id = users.id ORDER BY timestamp DESC LIMIT 10;"
    )
    result = cursor.fetchall()
    if len(result) == 0:
        return None
    if original_rows:
        return result
    data = [list(row) for row in result]
    for row in data:
        row[1] = time.mktime(row[1].timetuple())
        row.append(get_discord_from_leetcode(row[0]))
        row[0], row[1], row[2] = row[2], row[0], row[1]
    return data


@with_db
def get_points(cursor, problem_slug=None):
    if not problem_slug:
        return -1
    cursor.execute(
        "SELECT points FROM difficulty WHERE titleslug = %s;",
        (problem_slug,),
    )
    result = cursor.fetchall()
    return result[0][0] if result else None


@with_db
def get_last_reset(cursor):
    cursor.execute("SELECT last_reset, reset_interval FROM reset;")
    result = cursor.fetchall()
    return result if result else None


@with_db
def get_admins(cursor):
    cursor.execute("SELECT discord_id FROM admins;")
    result = cursor.fetchall()
    return result if result else []


@with_db
def add_admin(cursor, discord_id):
    try:
        cursor.execute(
            "INSERT INTO admins (discord_id) VALUES (%s);",
            (discord_id,),
        )
        return True
    except Exception as e:
        return False, str(e)


@with_db
def check_if_user_did_problem(cursor, discord_user, problem_name):
    leetcode_username = get_leetcode_from_discord(discord_user)
    cursor.execute(
        "SELECT * FROM user_submissions WHERE user_id = (SELECT id FROM users WHERE LOWER(username) = LOWER(%s)) AND problem_name = %s;",
        (leetcode_username, problem_name),
    )

    return cursor.fetchall()


@with_db
def check_if_user_busy(cursor, discord_user):

    leetcode_username = get_leetcode_from_discord(discord_user)
    cursor.execute(
        "SELECT busy FROM challenge WHERE id = (SELECT id FROM users WHERE LOWER(username) = LOWER(%s));",
        (leetcode_username,),
    )
    result = cursor.fetchall()
    if result == []:
        # initialize the user
        # use their id using leetcode user name, set wins, losses, quits all to 0 and busy to false
        cursor.execute(
            "INSERT INTO challenge (id, wins, losses, quits, busy) VALUES ((SELECT id FROM users WHERE LOWER(username) = LOWER(%s)), 0, 0, 0, false);",
            (leetcode_username,),
        )
        return False
    return result[0][0]


@with_db
def set_user_busy(cursor, discord_user, busy=True):
    leetcode_username = get_leetcode_from_discord(discord_user)
    cursor.execute(
        "UPDATE challenge SET busy = %s WHERE id = (SELECT id FROM users WHERE LOWER(username) = LOWER(%s));",
        (busy, leetcode_username),
    )
    return True


@with_db
def add_loss(cursor, discord_user):
    leetcode_username = get_leetcode_from_discord(discord_user)
    cursor.execute(
        """
        UPDATE challenge
        SET losses = losses + 1
        WHERE id = (SELECT id FROM users WHERE LOWER(username) = LOWER(%s))
        RETURNING losses;
        """,
        (leetcode_username,),
    )
    updated_losses = cursor.fetchone()[0]
    return updated_losses


@with_db
def add_win(cursor, discord_user):
    leetcode_username = get_leetcode_from_discord(discord_user)
    cursor.execute(
        """
        UPDATE challenge
        SET wins = wins + 1
        WHERE id = (SELECT id FROM users WHERE LOWER(username) = LOWER(%s))
        RETURNING wins;
        """,
        (leetcode_username,),
    )
    updated_wins = cursor.fetchone()[0]
    return updated_wins


@with_db
def add_quit(cursor, discord_user):
    leetcode_username = get_leetcode_from_discord(discord_user)
    cursor.execute(
        """
        UPDATE challenge
        SET quits = quits + 1
        WHERE id = (SELECT id FROM users WHERE LOWER(username) = LOWER(%s))
        RETURNING quits;
        """,
        (leetcode_username,),
    )
    updated_quits = cursor.fetchone()[0]
    return updated_quits


@with_db
def get_wins(cursor, discord_user):
    leetcode_username = get_leetcode_from_discord(discord_user)
    cursor.execute(
        """
        SELECT wins
        FROM challenge
        WHERE id = (SELECT id FROM users WHERE LOWER(username) = LOWER(%s));
        """,
        (leetcode_username,),
    )
    result = cursor.fetchone()
    return result[0] if result else 0


@with_db
def get_losses(cursor, discord_user):
    leetcode_username = get_leetcode_from_discord(discord_user)
    cursor.execute(
        """
        SELECT losses
        FROM challenge
        WHERE id = (SELECT id FROM users WHERE LOWER(username) = LOWER(%s));
        """,
        (leetcode_username,),
    )
    result = cursor.fetchone()
    return result[0] if result else 0


@with_db
def get_quits(cursor, discord_user):
    leetcode_username = get_leetcode_from_discord(discord_user)
    cursor.execute(
        """
        SELECT quits
        FROM challenge
        WHERE id = (SELECT id FROM users WHERE LOWER(username) = LOWER(%s));
        """,
        (leetcode_username,),
    )
    result = cursor.fetchone()
    return result[0] if result else 0


@with_db
def get_user_challenge_stats(cursor, discord_user):
    leetcode_username = get_leetcode_from_discord(discord_user)
    cursor.execute(
        """
        SELECT wins, losses, quits
        FROM challenge
        WHERE id = (SELECT id FROM users WHERE LOWER(username) = LOWER(%s));
        """,
        (leetcode_username,),
    )
    result = cursor.fetchone()
    return list(result) if result else [0, 0, 0]


@with_db
def CLEAR_ALL_POINTS(cursor, reset_interval):
    cursor.execute("UPDATE reset SET last_reset = CURRENT_DATE;")
    cursor.execute("UPDATE points SET points = 0;")
