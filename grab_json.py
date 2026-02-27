import mysql.connector
import time
import gzip
import json
import os
from Mysql_con_cre_ins import connection
a = time.time()


conn = connection()
cursor = conn.cursor()
path = r'C:\Users\yash.limbasiya\Desktop\grab_food_pages' # file path to the JSON data

def grab_json(file_path): # load JSON data from the specified file path
    files_app = []
    for files in os.listdir(file_path):
        fullpath = os.path.join(file_path, files)
        try:
            with gzip.open(fullpath, 'rt', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    files_app.append(data)
        except Exception as e:
            print("Error in file:", files, e)
    return files_app

def main(data): # Process the JSON data to extract restaurant and menu information

    d_data = []
    for extract in data:

        if not isinstance(extract, dict):
            continue

        merchant = extract.get('merchant',{})
        if not isinstance(merchant, dict):
            continue

        menu = merchant.get('menu',{})
        if not isinstance(menu, dict):
            continue

        categories_path = menu.get('categories',[])

        main_menu_title = dict()
        main_dict = dict()

        for category in categories_path:

            if not isinstance(category, dict):
                continue

            items_path = category.get('items', [])
            main_name = category.get('name')

            main_menu_title[main_name] = []

            for item in items_path:
                item_id = item.get('ID')
                item_name = item.get('name')
                item_availability = item.get('available')

                raw_price = item.get('priceV2', {}).get('amountDisplay', 0)

                try:
                    item_price = float(str(raw_price).replace("RM","").strip())
                except:
                    item_price = 0.0

                item_imgHref = item.get('imgHref')
                item_description = item.get('description')

                menu_dict = {
                    'item_id': item_id,
                    'name': item_name,
                    'available': item_availability,
                    'RM price': item_price,
                    'imgHref': item_imgHref,
                    'description': item_description
                }
                main_menu_title[main_name].append(menu_dict)

        main_dict['name'] = merchant.get('name')
        main_dict['id'] = merchant.get('ID')
        main_dict['cuisine'] = merchant.get('cuisine')
        main_dict['restaurant_logo'] = merchant.get('photoHref')
        main_dict['timeZone'] = merchant.get('timeZone')
        main_dict['estimated_delivery_time'] = merchant.get('ETA')
        main_dict['timing'] = merchant.get('openingHours', {})
        main_dict['distanceInKm'] = merchant.get('distanceInKm')
        main_dict['tips'] = merchant.get('sofConfiguration', {}).get('tips')
        main_dict['rating'] = merchant.get('rating')
        main_dict['voteCount'] = merchant.get('voteCount')
        main_dict['deliverBy'] = merchant.get('deliverBy')
        main_dict['radius'] = merchant.get('radius')
        main_dict['menu'] = main_menu_title

        d_data.append(main_dict)

    return d_data

def dump_json(data): # Dump the processed data into a new JSON file
    with open("final_cleaned.json", 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

json_load = grab_json(path)
result = main(json_load)
dump_json(result)

cursor.execute("CREATE DATABASE IF NOT EXISTS grabfood_db")
cursor.execute("USE grabfood_db")

# Create Tables restaurant
cursor.execute("""CREATE TABLE IF NOT EXISTS restaurant (
    id VARCHAR(30) PRIMARY KEY,
    name VARCHAR(255),
    address VARCHAR(255),
    restaurant_logo TEXT,
    timeZone VARCHAR(100),
    estimated_delivery_time INT,
    distanceInKm FLOAT,
    tips TEXT,
    rating FLOAT,
    voteCount INT,
    deliverBy VARCHAR(50),
    radius INT,
    open_status BOOLEAN,
    displayedHours VARCHAR(50),
    sun VARCHAR(50),
    mon VARCHAR(50),
    tue VARCHAR(50),
    wed VARCHAR(50),
    thu VARCHAR(50),
    fri VARCHAR(50),
    sat VARCHAR(50)
);""")

# Create Tables menu_item
# Composite PRIMARY KEY (item_id, category_name) allows same item in multiple categories
cursor.execute("""CREATE TABLE IF NOT EXISTS menu_item (
    item_id VARCHAR(50),
    restaurant_id VARCHAR(30),
    category_name VARCHAR(100),
    name VARCHAR(255),
    available BOOLEAN,
    price_rm FLOAT,
    imgHref TEXT,
    description TEXT,
    PRIMARY KEY (item_id, category_name),
    FOREIGN KEY (restaurant_id) REFERENCES restaurant(id)
);""")

# Insert Restaurant Data
# Using "INSERT IGNORE" to prevent crash if ID already exists
restaurant_insert_query = """
INSERT IGNORE INTO restaurant (
    id, name, address, restaurant_logo, timeZone,
    estimated_delivery_time, distanceInKm, tips,
    rating, voteCount, deliverBy, radius,
    open_status, displayedHours, sun, mon, tue, wed, thu, fri, sat
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


# batches 
batch_procesing = 2000
# 1. Define queries ONCE outside the loop
restaurant_insert_query = """
INSERT IGNORE INTO restaurant (
    id, name, address, restaurant_logo, timeZone,
    estimated_delivery_time, distanceInKm, tips,
    rating, voteCount, deliverBy, radius,
    open_status, displayedHours, sun, mon, tue, wed, thu, fri, sat
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

menu_insert_query = """
INSERT IGNORE INTO menu_item (
    item_id, restaurant_id, category_name,
    name, available, price_rm, imgHref, description
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
"""

# 2. Lists to hold batches
restaurant_data_list = []
menu_data_list = []

for restaurant in result:

    timing = restaurant.get("timing", {})
    
    # Prepare Restaurant Row
    restaurant_data_list.append((
        restaurant.get("id"),
        restaurant.get("name"),
        restaurant.get("cuisine"),
        restaurant.get("restaurant_logo"),
        restaurant.get("timeZone"),
        restaurant.get("estimated_delivery_time"),
        restaurant.get("distanceInKm"),
        str(restaurant.get("tips")),
        restaurant.get("rating"),
        restaurant.get("voteCount"),
        restaurant.get("deliverBy"),
        restaurant.get("radius"),
        timing.get("open"),
        timing.get("displayedHours"),
        timing.get("sun"),
        timing.get("mon"),
        timing.get("tue"),
        timing.get("wed"),
        timing.get("thu"),
        timing.get("fri"),
        timing.get("sat")
    ))

    # Prepare Menu Rows
    for category_name, items in restaurant.get("menu", {}).items():
        for item in items:
            menu_data_list.append((
                item.get("item_id"),
                restaurant.get("id"),
                category_name,
                item.get("name"),
                item.get("available"),
                item.get("RM price"),
                item.get("imgHref"),
                item.get("description")
            ))

# 3. Execute in Batches
# Fast insertion for restaurants
if restaurant_data_list:
    for i in range(0, len(restaurant_data_list), batch_procesing):
        cursor.executemany(restaurant_insert_query, restaurant_data_list[i:i+batch_procesing])

# Fast insertion for menu items
if menu_data_list:
    # If menu_data_list is huge (e.g., 50k+ rows), process in chunks of 2000
    for i in range(0, len(menu_data_list), 500):
        cursor.executemany(menu_insert_query, menu_data_list[i:i+2000])

conn.commit()

print(" Data inserted into Database!")
b = time.time()
print("Execution Time:", b-a, "seconds")
cursor.close()
conn.close()