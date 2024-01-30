import configparser
import pandas as pd


# CONFIG #############################################################################
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES ########################################################################
staging_events_table_drop = 'DROP TABLE IF EXISTS "staging_events_table"'
staging_songs_table_drop = 'DROP TABLE IF EXISTS "staging_songs_table"'
songplay_table_drop = 'DROP TABLE IF EXISTS "songplays"'
user_table_drop = 'DROP TABLE IF EXISTS "users"'
song_table_drop = 'DROP TABLE IF EXISTS "songs"'
artist_table_drop = 'DROP TABLE IF EXISTS "artists"'
time_table_drop = 'DROP TABLE IF EXISTS "time"'



# CREATE TABLES #######################################################################
staging_events_table_create= ("""
CREATE TABLE "staging_events_table" (                          
    "event_id" BIGINT IDENTITY(0,1) NOT NULL, 
    "artist" VARCHAR(256),
    "auth" VARCHAR(256),
    "firstName" VARCHAR(128),
    "gender" VARCHAR(8),
    "itemInSession" INTEGER,
    "lastName" VARCHAR(128),
    "length" FLOAT,
    "level" VARCHAR(16),
    "location" VARCHAR(256),
    "method" VARCHAR(16),
    "page" VARCHAR(32),
    "registration" BIGINT, 
    "sessionId" INTEGER,
    "song" VARCHAR(256), 
    "status" INTEGER,
    "ts" BIGINT,
    "userAgent" VARCHAR(256), 
    "userId" INTEGER,
    primary key(event_id)
);                            
""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs_table" (                          
    "song_id" VARCHAR(256) NOT NULL,
    "num_songs" INTEGER,
    "artist_id" VARCHAR(256),
    "artist_latitude" FLOAT,
    "artist_longitude" FLOAT,
    "artist_location" VARCHAR(1024),
    "artist_name" VARCHAR(1024),
    "title" VARCHAR(256),
    "duration" FLOAT,
    "year" INTEGER,
    primary key(song_id)
);
""")

# songplays table is a FACT table and contains large no. of records, so it is better to have a distkey and sortkey as well
songplay_table_create = ("""
CREATE TABLE "songplays" (                          
    "songplay_id" BIGINT NOT NULL,
    "start_time" TIMESTAMP,
    "user_id" INTEGER,
    "level" VARCHAR(16),
    "song_id" VARCHAR(256),
    "artist_id" VARCHAR(256),
    "session_id" INTEGER,
    "location" VARCHAR(256), 
    "user_agent" VARCHAR(256), 
    primary key(songplay_id)
)
distkey(songplay_id) 
sortkey(songplay_id,start_time);             
""")

# users table is a DIMENSION table, so it is better to have sortkey only and diststyle as "all"to imporve joining performance
user_table_create = ("""
CREATE TABLE "users" ( 
    "id" INTEGER IDENTITY(0,1) NOT NULL,                         
    "user_id" INTEGER, 
    "first_name" VARCHAR(128),
    "last_name" VARCHAR(128),
    "gender" VARCHAR(8),
    "level" VARCHAR(16),
    primary key(id)
) diststyle all 
sortkey(user_id, first_name, last_name);                    
""")

# songs table is a DIMENSION table, so it is better to have sortkey only and diststyle as "all"to imporve joining performance
song_table_create = ("""
CREATE TABLE "songs" ( 
    "id" INTEGER IDENTITY(0,1) NOT NULL,    
    "song_id" VARCHAR(256) NOT NULL,
    "title" VARCHAR(256),
    "artist_id" VARCHAR(256),
    "year" INTEGER,
    "duration" FLOAT,
    primary key(id)                         
) diststyle all 
sortkey(song_id, year, title);  
""")

# artists table is a DIMENSION table, so it is better to have sortkey only and diststyle as "all"to imporve joining performance
artist_table_create = ("""
CREATE TABLE "artists" ( 
    "id" INTEGER IDENTITY(0,1) NOT NULL,    
    "artist_id" VARCHAR(256) NOT NULL,
    "name" VARCHAR(1024),
    "location" VARCHAR(1024),
    "latitude" FLOAT,
    "longitude" FLOAT,
    primary key(id)                         
) diststyle all 
sortkey(artist_id, name);  
""")

# time table is a DIMENSION table, so it is better to have sortkey only and diststyle as "all"to imporve joining performance
time_table_create = ("""
CREATE TABLE "time" ( 
    "start_time" TIMESTAMP NOT NULL,
    "hour" INTEGER,
    "day" INTEGER,
    "week" INTEGER,
    "month" INTEGER, 
    "year" INTEGER, 
    "weekday" INTEGER,
    primary key(start_time)                         
) diststyle all 
sortkey(start_time, year, month, day, hour, weekday);  
""")

# LOAD STAGING TABLES
staging_events_copy = ("""
copy staging_events_table
from '{}'
iam_role '{}' 
json '{}';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs_table
from '{}'
iam_role '{}' 
json 'auto ignorecase';    
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])



# LOAD FINAL TABLES #######################################################################
# The ts column is in epoch in milliseconds, need to convert it to timestamp format.
# Ref https://www.fernandomc.com/posts/redshift-epochs-and-timestamps/
songplay_table_insert = ("""
insert into songplays("songplay_id", "start_time", "user_id", "level", "song_id", 
"artist_id", "session_id", "location", "user_agent") 
select e.event_id, timestamp 'epoch' + (e.ts/1000) * interval '1 second' AS start_time, 
        e.userId, e.level, s.song_id, s.artist_id, e.sessionId,
        e.location, e.userAgent 
from staging_events_table e JOIN staging_songs_table s 
on(e.artist=s.artist_name AND e.song=s.title)
""")

# We need to use 'distinct on' clause to have a distinct set of users with unique userId 
# However, redshift does not support "distinct on" expression, and we need to use the "first_value" window function
# as a workaround to achieve the same result as "distinct on". Note there are duplicated userId with multiple values in column "level"
# Ref1: https://community.sisense.com/t5/knowledge/fixing-distinct-on-not-supported/ta-p/8984 
# Ref2: https://docs.aws.amazon.com/redshift/latest/dg/r_WF_first_value.html
# user_table_insert = ("""
# insert into users("user_id", "first_name", "last_name", "gender", "level") 
# select distinct userId, firstName, lastName, gender, 
# first_value(level) over(partition by userId order by level rows between unbounded preceding and unbounded following) as level 
# from staging_events_table where userId is not null order by userId
# """)
user_table_insert = ("""
insert into users("user_id", "first_name", "last_name", "gender", "level") 
select userId, firstName, lastName, gender, level 
from staging_events_table 
""")

# We need to use 'distinct on' clause to have a distinct set of songs with unique song_id 
# However, redshift does not support "distinct on" expression, and we need to use the "first_value" window function as a workaround
# song_table_insert = ("""
# insert into songs("song_id", "title", "artist_id", "year", "duration") 
# select distinct song_id, 
# first_value(title) over(partition by song_id order by title rows between unbounded preceding and unbounded following) as title, 
# artist_id, year, duration from staging_songs_table 
# """)
song_table_insert = ("""
insert into songs("song_id", "title", "artist_id", "year", "duration") 
select distinct song_id, title, 
artist_id, year, duration from staging_songs_table 
""")

# We need to use 'distinct on' clause to have a distinct set of artists with unique artist_id
# However, redshift does not support "distinct on" expression, and we need to use the "first_value" window function as a workaround
# It's found that under the same artist_id, "artist_name" will have duplicate values.
# artist_table_insert = ("""
# insert into artists("artist_id", "name", "location", "latitude", "longitude") 
# select distinct artist_id, 
# first_value(artist_name) over(partition by artist_id order by artist_name rows between unbounded preceding and unbounded following) as artist_name, 
# artist_location, artist_latitude, artist_longitude from staging_songs_table 
# order by artist_id, artist_name
# """)
artist_table_insert = ("""
insert into artists("artist_id", "name", "location", "latitude", "longitude") 
select distinct artist_id, artist_name, artist_location, 
artist_latitude, artist_longitude from staging_songs_table 
""")

time_table_insert = ("""
insert into time("start_time", "hour", "day", "week", "month", "year", "weekday") 
select distinct 
s.start_time as start_time,  
extract('hour' from  s.start_time)::INTEGER as hour,
extract('day' from  s.start_time)::INTEGER as day,
extract('week' from  s.start_time)::INTEGER as week,
extract('month' from  s.start_time)::INTEGER as month,
extract('year' from  s.start_time)::INTEGER as year,
extract('weekday' from  s.start_time)::INTEGER as weekday 
from songplays as s
""")



# RETRIEVE DATA #######################################################################
loaded_tables= ["staging_events_table", "staging_songs_table", "songplays", "users", "songs", "artists", "time"]
def retrieve_counts(cur, conn, table_name=None, query_tuple=None):
    if table_name is not None:
        query = f"select count(*) from {table_name}"
        cur.execute(query)
        rows = cur.fetchall()
        print(f"Retrieve the no. of records in {table_name} : {rows[0][0]}")

        
    if query_tuple is not None:
        print(query_tuple[1])
        query = query_tuple[0]
        cur.execute(query)
        rows = cur.fetchall()
        col_names = [d[0] for d in cur.description] 
        df = pd.DataFrame(rows, columns=col_names)
        print(df)
        
        
        
# Top 10 ever most played songs
top10_most_played_songs_retrieve = (("""select s.title as song_title, count(*) as play_count
from songplays p join songs s on(p.song_id = s.song_id) 
group by s.title order by play_count desc limit 10"""), "Top 10 ever most played songs : ")

# Top 5 highest usage time of day by hour for songs
top5_busy_hours_retrieve = (("""select t.hour hour_of_day, count(*) as play_count 
from songplays p join time t on(p.start_time = t.start_time) 
group by t.hour order by play_count desc limit 5"""), "Top 5 highest usage time of day by hour for songs : ")

# Top 10 hottest artists with most played songs
top10_most_played_songs_retrieve = (("""select  a.name as artist_name, count(*) as play_count 
from songplays p join artists a on(p.artist_id = a.artist_id) 
group by a.name order by play_count desc limit 10"""), "Top 10 hottest artists with most played songs : ")
 


# QUERY LISTS #######################################################################
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
topN_query_tuples = [top10_most_played_songs_retrieve, top5_busy_hours_retrieve, top10_most_played_songs_retrieve]