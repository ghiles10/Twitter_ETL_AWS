create_tweet_table = """
CREATE TABLE IF NOT EXISTS ghiles.tweets 
(
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    text VARCHAR,
    favorite_count VARCHAR,
    retweet_count VARCHAR,
    date_creation TIMESTAMP,
    date DATE
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
    favorite_count VARCHAR,
    retweet_count VARCHAR,
    date_creation TIMESTAMP,
    date DATE
)
SORTKEY ("user");
"""

