from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
from pyspark.sql.functions import broadcast

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("Spark").getOrCreate()

#Dataframes
#matches : a row for every match
matches_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
matches_df.show()

#match_details : a row for every players performance in a match
match_details_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
match_details_df.show()

#medals_matches_players : a row for every medal type a player gets in a match
medals_matches_players_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")
medals_matches_players_df.show()

#medals : a row for every medal type
medals_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
medals_df.show()

#maps
maps_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")
maps_df.show()

#Create a new DF from match_details_df with selected columns
player_performance_df = match_details_df.select(
    "match_id",
    "player_gamertag",
    "player_total_kills",
    "player_total_headshots",
    "player_total_weapon_damage",
    "player_total_assists",
    "player_total_deaths",
    "player_rank_on_team",
    "team_id"
)

player_performance_df.show(truncate=False)

# Build a Spark job that
# Disabled automatic broadcast join with spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
# Step 2: Disable Automatic Broadcast Joins
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
print("Disabled automatic broadcast join")

match_details_df = spark.read.csv("/home/iceberg/data/match_details.csv", header=True, inferSchema=True)
matches_df = spark.read.csv("/home/iceberg/data/matches.csv", header=True, inferSchema=True)
medal_matches_players_df = spark.read.csv("/home/iceberg/data/medals_matches_players.csv", header=True, inferSchema=True)
medals_df = spark.read.csv("/home/iceberg/data/medals.csv", header=True, inferSchema=True)
maps_df = spark.read.csv("/home/iceberg/data/maps.csv", header=True, inferSchema=True)

# Explicitly Broadcast Join Medals and Maps
medals_maps_joined_df = medals_df.join(
    broadcast(maps_df),
    medals_df["name"] == maps_df["name"],
    "inner"
)

medals_matches_joined_df.show(truncate=False)

# Perform an Explicit Broadcast Join
# Broadcasting medals_df (smaller table) for better performance
medals_matches_joined_df = medal_matches_players_df.join(
    broadcast(medals_df),
    medal_matches_players_df["medal_id"] == medals_df["medal_id"],
    "inner"
)

medals_matches_joined_df.show(truncate=False)

# Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets
spark = SparkSession.builder \
    .appName("BucketJoinExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

match_details_df.write.bucketBy(16, "match_id") \
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("default.match_details_bucketed")

matches_df.write.bucketBy(16, "match_id") \
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("default.matches_bucketed")

medal_matches_players_df.write.bucketBy(16, "match_id") \
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("default.medal_matches_players_bucketed")

match_details_bucketed = spark.read.table("default.match_details_bucketed")
matches_bucketed = spark.read.table("default.matches_bucketed")
medal_matches_players_bucketed = spark.read.table("default.medal_matches_players_bucketed")

joined_df = match_details_bucketed.alias("md") \
    .join(matches_bucketed.alias("m"), col("md.match_id") == col("m.match_id")) \
    .join(medal_matches_players_bucketed.alias("mp"), col("md.match_id") == col("mp.match_id"))
joined_df.show(truncate=False)

# Aggregate the joined data frame to figure out questions like:

# Which player averages the most kills per game?
player_avg_kills = joined_df.groupBy(col("mp.player_gamertag")) \
    .agg(avg("md.player_total_kills").alias("avg_kills")) \
    .orderBy(desc("avg_kills"))

print("Player who averages the most kills per game:")
player_avg_kills.show(truncate=False)

# Which playlist gets played the most?
most_played_playlist = joined_df.groupBy("m.playlist_id") \
    .agg(count("*").alias("times_played")) \
    .orderBy(desc("times_played"))

print("Playlist that gets played the most:")
most_played_playlist.show(truncate=False)

# Which map gets played the most?
most_played_map = joined_df.groupBy("m.mapid") \
    .agg(count("*").alias("times_played")) \
    .orderBy(desc("times_played"))

print("Map that gets played the most:")
most_played_map.show(truncate=False)
                                                                                    
# Which map do players get the most Killing Spree medals on?

#Filter for "Killing Spree" Medals
# Assuming "Killing Spree" corresponds to a specific `medal_id`
killing_spree_medals = medal_matches_players_df.alias("medal_matches").join(
    match_details_df.alias("match_details"),
    col("medal_matches.match_id") == col("match_details.match_id"),
    "inner"
)

#Join the Results with Matches and Maps
killing_spree_data = killing_spree_medals.join(
    broadcast(matches_df.alias("matches")),
    col("medal_matches.match_id") == col("matches.match_id"),
    "inner"
).join(
    broadcast(maps_df.alias("maps")),
    col("matches.mapid") == col("maps.mapid"),
    "inner"
)

#Aggregate Data by Map
most_killing_spree_map = killing_spree_data.groupBy("maps.name") \
    .agg(count("medal_matches.medal_id").alias("killing_spree_count")) \
    .orderBy(desc("killing_spree_count"))

print("Map where players get the most Killing Spree medals:")
most_killing_spree_map.show(truncate=False)

#Optimize Data Size

from pyspark.sql.functions import avg, count, col, desc
# Step 1: Aggregate the Data
aggregated_df = joined_df.groupBy("m.playlist_id", "m.mapid") \
    .agg(
        avg("md.player_total_kills").alias("avg_kills"),
        count("*").alias("total_plays")
    )

# Define a Function to Measure Partition Sizes
def partition_sizes(df):
    return df.rdd.mapPartitions(lambda p: [sum(len(str(row)) for row in p)]).collect()

def clean_data(df):
    return df.dropDuplicates()

cleaned_df = clean_data(aggregated_df)

# With the aggregated data set
# Try different .sortWithinPartitions to see which has the smallest data size (hint: playlists and maps are both very low cardinality)
partition_results = []
columns_to_test = ["m.playlist_id", "m.mapid"]  # Only low-cardinality columns

for num_partitions in [4, 8, 16]:
    for column in columns_to_test:
        repartitioned_df = cleaned_df.repartition(num_partitions, col(column))
        sizes_before_sorting = partition_sizes(repartitioned_df)
        sorted_df = repartitioned_df.sortWithinPartitions(column)
        sizes_after_sorting = partition_sizes(sorted_df)
        partition_results.append({
            "column": column,
            "num_partitions": num_partitions,
            "sizes_before_sorting": sizes_before_sorting,
            "sizes_after_sorting": sizes_after_sorting,
            "total_size_before": sum(sizes_before_sorting),
            "total_size_after": sum(sizes_after_sorting)
        })

for result in partition_results:
    print(f"Column: {result['column']}, Partitions: {result['num_partitions']}")
    print(f"Partition sizes before sorting: {result['sizes_before_sorting']}")
    print(f"Partition sizes after sorting: {result['sizes_after_sorting']}")
    print(f"Total size before sorting: {result['total_size_before']} bytes")
    print(f"Total size after sorting: {result['total_size_after']} bytes")
    print()
best_result = min(partition_results, key=lambda x: x["total_size_after"])
print(f"Best column to sortWithinPartitions: {best_result['column']} with {best_result['num_partitions']} partitions")
