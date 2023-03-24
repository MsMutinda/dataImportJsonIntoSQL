import mysql.connector
import json
import random
import vars as config

category_ids = ['1', '3', '4', '6', '7', '8', '9']
store_branch_ids = ['3', '4', '16']

try:
    connection = mysql.connector.connect(host=config.DB_HOST, port=config.DB_PORT, user=config.DB_USER, password=config.DB_PASS, database=config.DB_NAME)

    connection.autocommit = False

    cursor = connection.cursor()

    with open('products_new.json', 'r') as datafile:
        dataArray = json.JSONDecoder().decode(datafile.read())
    
        numProductObjects = len(dataArray)
        successfulInserts = 0
        # for each product object, insert the product in table 1 then its price in table2
        for obj in dataArray:
            # 1. insert the product
            product_category_id = random.choice(category_ids)
            # product_category_id = '3'

            # a - check if exists first
            query01 = ("SELECT * FROM product where name=%s and brand=%s and product_category_id=%s") 
            query01_data = (obj['name'], obj['brand'], product_category_id)

            cursor.execute(query01, query01_data)
            
            productExists = cursor.fetchall()
            
            print('product check query run is: ', cursor._executed) # this returns the executed query
            print('the number of products that match the provide criteria are:', len(productExists))
            
            if len(productExists) == 0:
                query1 = ("INSERT INTO product "
                "(name, brand, image, unit_text, unit_int, more_info, product_category_id, creator_id)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

                query1_data = (obj['name'], obj['brand'], obj['image'], obj['unit_text'], obj['unit_int'], obj['more_info'], product_category_id, '1')

                cursor.execute(query1, query1_data)

                result1 = cursor.lastrowid
            else:
                result1 = productExists[0][0]


            # 2. using the inserted product id, save the product's price
            insertedProductID = result1

            store_branch_id = random.choice(store_branch_ids)

            # a - check if exists first
            query02 = ("SELECT * FROM product_store_branch_price where product_id=%s and store_branch_id=%s") 

            query02_data = (result1, store_branch_id)

            cursor.execute(query02, query02_data)

            productPriceExists = cursor.fetchall()

            print('product price check query run is: ' ,cursor._executed) # this returns the executed query
            print('the number of product prices that match the provide criteria are:', len(productPriceExists))
            

            if len(productPriceExists) == 0:
                query2 = ("INSERT INTO product_store_branch_price "
                "(product_id, price, store_branch_id, creator_id)"
                "VALUES (%s, %s, %s, %s)")

                query2_data = (insertedProductID, obj['price'], store_branch_id, '1')

                cursor.execute(query2, query2_data)
            
            result2 = cursor.lastrowid

            if result2 and result2 is not None:
                print("Product saved successfully!")

            successfulInserts += 1

            # # comment below after Fred's confirmation on random category and store_branch_ids
            # break



    print(successfulInserts)
    print(numProductObjects)

    
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
