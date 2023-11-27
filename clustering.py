from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import pandas as pd

embeddings_df = pd.read_csv("embeddings.csv")

embeddings_list = embeddings_df['embeddings'].tolist()

for embedding in embeddings_list:
  

embeddings_df['embeddings'] = embeddings_df['embeddings'].apply(eval).apply(np.array)

embeddings_list = embeddings_df['embeddings'].tolist()

cosine_sim_matrix = cosine_similarity(embeddings_list)

agg_clustering = AgglomerativeClustering(n_clusters=None,
                                        distance_threshold=0.1,
                                        affinity='precomputed',
                                        linkage='complete')

agg_clustering.fit(1 - cosine_sim_matrix)

cluster_labels = agg_clustering.labels_

unique_labels, counts = np.unique(cluster_labels, return_counts=True)

for label, count in zip(unique_labels, counts):
  print(f'Cluster {label}: {count} embeddings')