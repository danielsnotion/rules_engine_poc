import redis
import pandas as pd
import io


class RedisCache:
    _instance = None  # Singleton instance

    def __new__(cls, host='localhost', port=6379, db=0):
        if cls._instance is None:
            cls._instance = super(RedisCache, cls).__new__(cls)
            cls._instance.redis_client = redis.StrictRedis(host=host, port=port, db=db, decode_responses=False)
        return cls._instance

    def save_dataframe(self, key: str, df: pd.DataFrame):
        """
        Save Pandas DataFrame to Redis efficiently.

        Parameters:
            key (str): Redis key to store the DataFrame.
            df (pd.DataFrame): DataFrame to store.
        """
        df_bytes = io.BytesIO()
        df.to_parquet(df_bytes,index=False)  # Convert DataFrame to Parquet format
        df_bytes.seek(0)

        with self.redis_client.pipeline() as pipe:
            pipe.set(key, df_bytes.getvalue())
            pipe.execute()

    def get_dataframe(self, key: str) -> pd.DataFrame:
        """
        Retrieve Pandas DataFrame from Redis.

        Parameters:
            key (str): Redis key where DataFrame is stored.

        Returns:
            pd.DataFrame: Retrieved DataFrame.
        """
        data = self.redis_client.get(key)
        if data is None:
            raise KeyError(f"No data found in Redis for key: {key}")

        df_bytes = io.BytesIO(data)
        return pd.read_parquet(df_bytes)

    def close_connection(self):
        """Properly close the Redis connection."""
        if self._instance:
            self._instance.redis_client.close()
            self._instance = None


# Example Usage
if __name__ == "__main__":
    # Note:
    # pip install pandas redis msgpack numpy
    try:
        redis_cache = RedisCache()

        # Create a sample DataFrame
        df = pd.DataFrame({"id": [1, 2, 3], "value": [10.5, 20.3, 30.1]})

        # Save DataFrame
        redis_cache.save_dataframe("sample_df", df)
        print("DataFrame saved to Redis!")

        # Retrieve DataFrame
        retrieved_df = redis_cache.get_dataframe("sample_df")
        print("DataFrame retrieved from Redis:", retrieved_df)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        redis_cache.close_connection()
        print("Redis connection closed.")
