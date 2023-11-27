from milvus import default_server
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility
import subprocess
import numpy as np
import pandas as pd
import os

def create_milvus_collection(collection_name, dim):
      if utility.has_collection(collection_name):
          utility.drop_collection(collection_name)

      fields = [
      FieldSchema(name='relativefilepath', dtype=DataType.VARCHAR, description='file path relative to root directory ', max_length=1000, is_primary=True, auto_id=False),
      FieldSchema(name='embedding', dtype=DataType.FLOAT_VECTOR, description='embedding vectors', dim=dim)
      ]
      schema = CollectionSchema(fields=fields, description='spark log similarity')
      collection = Collection(name=collection_name, schema=schema)

      # create IVF_FLAT index for collection.
      index_params = {
          'metric_type':'IP',
          'index_type':"IVF_FLAT",
          'params':{"nlist":2048}
      }
      collection.create_index(field_name="embedding", index_params=index_params)
      return collection
    
def main():
  # Reset the vector database files
  print(subprocess.run(["rm -rf milvus-spark-logs-data"], shell=True))

  default_server.set_base_dir('milvus-spark-logs-data')
  default_server.start()

  try:
    connections.connect(alias='default', host='localhost', port=default_server.listen_port)   
    print(utility.get_server_version())

    # Create/Recreate the Milvus collection
    collection_name = 'spark_event_logs'
    collection = create_milvus_collection(collection_name, 384)

    print("Milvus database is up and collection is created")

    # Read KB documents in ./data directory and insert embeddings into Vector DB for each doc
    # The default embeddings generation model specified in this AMP only generates embeddings for the first 256 tokens of text.
    
    embeddings_df = pd.read_csv("embeddings.csv")

    embeddings_list = embeddings_df['embeddings'].tolist()
    embeddings_file = embeddings_df['title'].tolist()

    for file, embedding in zip(embeddings_file, embeddings_list):
        data = [[file], [embedding]]
        collection.insert(data)
        collection.flush()
        print('Total number of inserted embeddings is {}.'.format(collection.num_entities))
        print('Finished loading Knowledge Base embeddings into Milvus')

  except Exception as e:
    default_server.stop()
    raise (e)


  default_server.stop()


if __name__ == "__main__":
    main()
    
    
def create_connection():
    print(f"\nCreate connection...")
   if not connections.has_connection("default"):
      connections.connect(host=_HOST, port=_PORT):
   else:
     print(f"\ default connection exist:")
    print(f"\nList connections:")
    print(connections.list_connections())
create_connection()