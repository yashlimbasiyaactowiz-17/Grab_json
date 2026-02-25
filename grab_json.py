import json
import mysql.connector

# Database Connection (Ensure these details are correct for your setup)
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="actowiz"
)
cursor = conn.cursor()

path = 'grabfood.json' # file path to the JSON data

def grab_json(file_path): # load JSON data from the specified file path
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

def main(data): # Process the JSON data to extract restaurant and menu information
    categories_path = data.get('merchant', {}).get('menu', {}).get('categories', [])
    main_menu_title = dict()
    main_dict = dict()

    for category in categories_path:
        items_path = category.get('items', [])
        main_name = category.get('name')
        
        # Initialize the list for the category name
        main_menu_title[main_name] = []
        
        for item in items_path:
            item_id = item.get('ID')
            item_name = item.get('name')
            item_availability = item.get('available')
            
            # Fixed float conversion error
            raw_price = item.get('priceV2', {}).get('amountDisplay', 0)
            item_price = float(raw_price)
            
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

    merchant = data.get('merchant', {})
    main_dict['name'] = merchant.get('name')
    main_dict['id'] = merchant.get('ID')
    main_dict['cuisine'] = merchant.get('cuisine')
    main_dict['restaurant_logo'] = merchant.get('photoHref')
    main_dict['timeZone'] = merchant.get('timeZone')
    main_dict['estimated_delivery_time'] = merchant.get('ETA')
    main_dict['timing'] = merchant.get('openingHours')
    main_dict['distanceInKm'] = merchant.get('distanceInKm')
    main_dict['tips'] = merchant.get('sofConfiguration', {}).get('tips')
    main_dict['rating'] = merchant.get('rating')
    main_dict['voteCount'] = merchant.get('voteCount')
    main_dict['deliverBy'] = merchant.get('deliverBy')
    main_dict['radius'] = merchant.get('radius')
    main_dict['menu'] = main_menu_title
    
    return main_dict

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
    distanceInKm DECIMAL(10,3),
    tips TEXT,
    rating DECIMAL(3,1),
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
    price_rm DECIMAL(10,2),
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

timing = result.get("timing", {})
restaurant_values = (
    result.get("id"),
    result.get("name"),
    result.get("cuisine"),
    result.get("restaurant_logo"),
    result.get("timeZone"),
    result.get("estimated_delivery_time"),
    result.get("distanceInKm"),
    str(result.get("tips")),
    result.get("rating"),
    result.get("voteCount"),
    result.get("deliverBy"),
    result.get("radius"),
    timing.get("open"),
    timing.get("displayedHours"),
    timing.get("sun"),
    timing.get("mon"),
    timing.get("tue"),
    timing.get("wed"),
    timing.get("thu"),
    timing.get("fri"),
    timing.get("sat")
)

cursor.execute(restaurant_insert_query, restaurant_values)
conn.commit()

# Insert Menu Items
menu_insert_query = """
INSERT IGNORE INTO menu_item (
    item_id, restaurant_id, category_name,
    name, available, price_rm, imgHref, description
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
"""

restaurant_id = result.get("id")
menu_data = result.get("menu", {})

for category_name, items in menu_data.items():
    for item in items:
        menu_values = (
            item.get("item_id"),
            restaurant_id,
            category_name,
            item.get("name"),
            item.get("available"),
            item.get("RM price"),
            item.get("imgHref"),
            item.get("description")
        )
        cursor.execute(menu_insert_query, menu_values)

conn.commit()
print(" Data inserted into Database!")

cursor.close()
conn.close()