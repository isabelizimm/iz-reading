import requests
import polars as pl
raw_request = requests.get("https://api.fable.co/api/users/529c21d5-fa03-461f-9711-eb061628003f/reviews/?limit=100").json()
data = raw_request.get("results")

cols = ['book', 'rating', 'review', 'created_at']
df = pl.DataFrame(data)
df = df[cols].unnest('book')

new_cols = ['title', 'authors', 'page_count', 'published_date', 'price_usd', 'subjects', 'background_color', 'review_average', 'review_count', 'genres', 'rating', 'review', 'created_at', 'cover_image']
max_authors = df.select(pl.col("authors").list.len().max()).item()
max_genres = df.select(pl.col("genres").list.len().max()).item()

clean = df[new_cols].with_columns(
        (
          pl.col("authors")
          .list.eval(pl.element().struct.field("name"))
          .list.to_struct(fields=lambda i: f"author_{i+1}", upper_bound=max_authors)
        ),
        (
          pl.col("genres")
          .list.eval(pl.element().struct.field("name"))
          .list.to_struct(fields=lambda i: f"genre_{i+1}", upper_bound=max_genres)
        ),
        (
          pl.col("subjects")
          .map_elements(
            lambda x: [item for sublist in x for item in sublist] if x is not None else [],
            return_dtype=pl.List(pl.String)
          )
        ),
        (pl.col("created_at").cast(pl.Datetime)),
        (pl.col("rating").cast(pl.Float64)),
        ((pl.col("rating").cast(pl.Float64) - pl.col("review_average")).alias("rating_difference"))
      )\
    .unnest("authors")\
    .unnest("genres")\
    .filter(pl.col('created_at') >= pl.lit('2025-01-01').str.to_datetime())

old_books = pl.read_parquet('data/fable.parquet')

concat = pl.concat([clean, old_books]).unique()

concat.write_parquet('data/fable.parquet')

# OH NO NOT AS MANY AS IVE READ
# group by month

# books_2025 = clean.filter(pl.col('created_at') >= pl.lit('2025-01-01').str.to_datetime())
# books_2025.group_by(pl.col('created_at').dt.month)

# ah, this is only what ive given REVIEWS for, not a comprehensive list of everything ive read

