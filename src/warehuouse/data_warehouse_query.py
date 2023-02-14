create_tweet_table = """
CREATE TABLE IF NOT EXISTS ghiles.tweets 
(
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    text VARCHAR,
    favorite_count INT,
    retweet_count INT,
    date_creation VARCHAR,
    date VARCHAR
)
SORTKEY (date_creation);
"""

create_user_table = """
CREATE TABLE IF NOT EXISTS ghiles.users
(
    "user" TEXT, 
    PRIMARY KEY ("user"),
    description VARCHAR,
    following VARCHAR,
    followers VARCHAR, 
    favorite_count INT,
    retweet_count INT,
    date_creation VARCHAR,
    date VARCHAR
)
SORTKEY ("user");
"""

