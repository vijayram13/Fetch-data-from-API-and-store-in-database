"""
    This program is used to fetch data from api and store in the database(sqlite).

    Given:
        URL: https://dummyapi.io/data/v1/user
        app-id: 65f1ae5e489d3eea6df33c72
        Post URL: https://dummyapi.io/data/v1/user/{{user_id}}/post
        
    Note: first remove the users_data.db file to avoid the error, then run this program.

"""
import sqlite3
import requests

# Your app_id obtained after registration/login
app_id = '65f1ae5e489d3eea6df33c72'

# API endpoint to fetch user data
api_url = 'https://dummyapi.io/data/v1/user'

# Headers with app_id
headers = {'app-id': app_id}


# Function to fetch user data from API endpoint
def fetch_user_data_from_api():
    print ('Fetching user data from API...')

    # fetching user data 
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        print ('Data fetched SUCCESSFULLY.')

        return response.json()['data']
    else:
        print('Error:', response.status_code)
        return None
    
# Function to create SQLite database and store user data
def store_users_data(users_data):
    try:
        print('Storing users data...')
        
        # database connection
        conn = sqlite3.connect('users_data.db')
        c = conn.cursor()

        # Create table
        c.execute('''CREATE TABLE IF NOT EXISTS users
                    (id TEXT PRIMARY KEY, title TEXT, firstName TEXT, lastName TEXT, picture TEXT)''')

        # Insert data into table
        for user in users_data:
            c.execute('INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?, ?)', (
                user['id'],
                user['title'],
                user['firstName'],
                user['lastName'],
                user['picture']
            ))

        print('Data Stored in database SUCCESSFULLY')
    except:
        print('Failed to Store users in database')
    finally:
        # Commit changes and close connection
        conn.commit()
        conn.close()

# Function to fetch user data from the database
def fetch_users_from_db():
    try:
        print('Fetching users from database')

        # database connection
        conn = sqlite3.connect('users_data.db')
        c = conn.cursor()
        # users table
        c.execute('SELECT id FROM users')
        users = c.fetchall()

        return users
    except:
        print("Something Went Worng...")
    finally:
        conn.close()

# Function to fetch posts data for a user
def fetch_posts_of_user(user_id):
    api_url = f'https://dummyapi.io/data/v1/user/{user_id}/post'
    headers = {'app-id': app_id}
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()['data']
    else:
        print(f'Error fetching posts for user {user_id}: {response.status_code}')
        return []

# Function to store posts data in the database
def store_posts_in_db(user_id, posts):
    try:
        print("Loading posts in database...")

        # database connection
        conn = sqlite3.connect('users_data.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS posts
                    (post_id TEXT PRIMARY KEY,image TEXT, likes INTEGER, tags TEXT, text TEXT, publish_date TEXT, user_id TEXT)''')
        
        # insert posts in database
        for post in posts:
            c.execute('INSERT INTO posts VALUES (?, ?, ?, ?, ?, ?, ?)', (
                post['id'],
                post['image'],
                post['likes'],
                ','.join(post['tags']) if 'tags' in post else '',
                post['text'],
                post['publishDate'],
                user_id
                
            ))
        
    except:
        print ('Failed to Store posts in database.')
    finally:
        conn.commit()
        conn.close()

if __name__ == "__main__":
    # fetch users data from api
    users = fetch_user_data_from_api()

    # store users in database
    store_users_data(users)

    # userdata from database
    users_from_db = fetch_users_from_db()

    # store posts in database
    for user_id in users_from_db:
        user_id = user_id[0]  # Extracting user_id from tuple
        posts = fetch_posts_of_user(user_id)
        store_posts_in_db(user_id, posts)

