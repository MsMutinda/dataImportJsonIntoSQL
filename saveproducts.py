import mysql.connector
import json
import random
import vars as config

category_ids = ['1', '3', '4', '6', '7', '8', '9']
store_branch_ids = ['3', '4', '16']

try:
    connection = mysql.connector.connect(host=config.DB_HOST, port=config.DB_PORT, user=config.DB_USER, password=config.db.DB_PASS, database=config.DB_NAME)

    connection.autocommit = False

    cursor = connection.cursor()

    with open('products_new.json', 'r') as datafile:
        dataArray = json.JSONDecoder().decode(datafile.read())
    
        # for each product object, insert the product in table 1 then its price in table2
        for obj in dataArray:
            # 1. insert the product - check if exists first
            query1 = ("INSERT INTO product "
               "(name, brand, image, unit_text, unit_int, more_info, product_category_id, creator_id)"
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

            query1_data = (obj['name'], obj['brand'], obj['image'], obj['unit_text'], obj['unit_int'], obj['more_info'], random.choice(category_ids), '1')

            cursor.execute(query1, query1_data)

            result1 = cursor.lastrowid


            # 2. using the inserted product id, save the product's price - check if exists first
            insertedProductID = result1
            query2 = ("INSERT INTO product_store_branch_price "
               "(product_id, price, store_branch_id, creator_id)"
               "VALUES (%s, %s, %s, %s)")

            query2_data = (insertedProductID, obj['price'], random.choice(store_branch_ids), '1')

            cursor.execute(query2, query2_data)

            result2 = cursor.lastrowid
            

            print("Product saved successfully!")


            # uncomment below after Fred's confirmation on random category and store_branch_ids
            break



    # Commit your changes
    connection.commit()

# reverting changes if exception occurs
except mysql.connector.Error as error:
    print("Failed to insert record to database. Rollback: {}".format(error))
    connection.rollback()

finally:
    # close db connection
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("connection is closed")
