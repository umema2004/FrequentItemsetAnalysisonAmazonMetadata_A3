from collections import defaultdict
from itertools import combinations
from sklearn.metrics.pairwise import cosine_similarity
from kafka import KafkaConsumer
import json

bootstrap_servers = ['localhost:9092']
topic = 'pp-topic'

products_list = []

consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Compute category similarity between two items
def compute_similarity(categories1, categories2):
    categories1_set = set(categories1)
    categories2_set = set(categories2)
    intersection = len(categories1_set.intersection(categories2_set))
    union = len(categories1_set.union(categories2_set))
    return intersection / union if union != 0 else 0

# Step 1: Hashing
def hash_function(item1, item2, table_size):
    return (hash(item1["title"] + item2["title"]) % table_size)


# Step 2: Bitmap
def create_bitmap(dataset, hash_table_size, support_threshold):
    bitmap = [0] * hash_table_size
    hash_table = defaultdict(int)

    for i in range(len(dataset)):
        for j in range(i + 1, len(dataset)):
            hash_val = hash_function(dataset[i], dataset[j], hash_table_size)
            hash_table[hash_val] += 1

    for hash_val, count in hash_table.items():
        if count >= support_threshold:
            bitmap[hash_val] = 1

    return bitmap

# Step 3: Prune Candidates
def prune_candidates(dataset, bitmap, hash_table_size, support_threshold):
    candidate_pairs = defaultdict(int)

    for i in range(len(dataset)):
        for j in range(i + 1, len(dataset)):
            hash_val = hash_function(dataset[i], dataset[j], hash_table_size)
            if bitmap[hash_val]:
                similarity = compute_similarity(dataset[i]["category"], dataset[j]["category"])
                candidate_pairs[(dataset[i]["title"], dataset[j]["title"])] = similarity

    frequent_pairs = [(pair, similarity) for pair, similarity in candidate_pairs.items() if similarity >= support_threshold]

    return frequent_pairs


# Step 4: Count Itemsets
def count_itemsets(dataset, frequent_pairs):
    itemsets = defaultdict(int)

    for item in dataset:
        itemsets[(item["title"],)] += 1

    for pair, similarity in frequent_pairs:
        itemsets[pair] = similarity

    return itemsets

# PCY Algorithm
hash_table_size = 10  # Adjust the size based on your dataset
support_threshold = 0.5  # Adjust as needed


for message in consumer:
    product_info = message.value
    products_list.append(product_info)


#    print(count)

    if len(products_list) == 10:
        bitmap = create_bitmap(products_list, hash_table_size, support_threshold)
        frequent_pairs = prune_candidates(products_list, bitmap, hash_table_size, support_threshold)
        itemsets = count_itemsets(products_list, frequent_pairs)

        print("Frequent Item Pairs with Similarity:")
        for itemset, similarity in itemsets.items():
            if len(itemset) > 1:
                print(itemset, ":", similarity)
                print('\n')
        products_list = []  # Clear the list after printing