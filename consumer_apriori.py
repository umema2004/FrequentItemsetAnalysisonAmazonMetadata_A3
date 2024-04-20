import json
from kafka import KafkaConsumer
from mlxtend.frequent_patterns import apriori, association_rules
import pandas as pd

bootstrap_servers = ['localhost:9092']
topic = 'pp-topic'

products_list = []

consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
count=0

def convert_to_tuples(item):
    converted_item = item.copy()
    converted_item['category'] = tuple(item['category'])
    converted_item['also_buy'] = tuple(item['also_buy'])
    #print('tuple ban gaya')
    return converted_item

# Function to calculate similarity between two products
def calculate_similarity(product1, product2):
    # Calculate similarity based on common attributes
    common_attributes = set(product1.items()) & set(product2.items())
    similarity = len(common_attributes) / len(set(product1.items()) | set(product2.items()))
    return similarity


# Function to generate frequent itemsets using Apriori algorithm
def apriori(items, min_support):
    num_transactions = len(items)
    frequent_itemsets = []
    for i, item1 in enumerate(items):
        for j, item2 in enumerate(items):
            if i != j:
                similarity = calculate_similarity(item1, item2)
                if similarity >= min_support:
                    frequent_itemsets.append(({i, j}, item1['title'], item2['title'], similarity))
    return frequent_itemsets

min_support = 0.07

for message in consumer:
    product_info = message.value
    products_list.append(product_info)

    count += 1
#    print(count)

    if len(products_list) == 10:
        products_list = [convert_to_tuples(item) for item in products_list]
        frequent_itemsets = apriori(products_list, min_support)
        print("Frequent Itemsets:")
        for itemset, title1, title2, similarity in frequent_itemsets:
            print("Itemset:", title1, ',' , title2)
            #print("Itemset:", itemset)
            #print("Similarity:", similarity)

        products_list = []  # Clear the list after printing
        count=0