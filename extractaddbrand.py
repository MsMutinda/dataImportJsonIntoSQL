import csv, re, json

productsjson = []
# columnames = []

with open('products.csv', 'r') as csvfile:
    datareader = csv.reader(csvfile)
    for row in datareader:
        product = {}

        for i in range(0, len(row)):
            # print(row[i])
            product['more_info'] = 'product url: ' + row[0]
            product['image'] = row[1]
            product['name'] = row[2]
            product['brand'] = row[2].split(' ')[0]
            product['price'] = row[3].replace('KES ', '')

            text = ''
            num = ''

            row[4] = row[4].split('x')[0]

            for j in row[4]:
                if j.isdigit():
                    # # when unit int is picked as 0 or "", set a default of 1
                    # if j == '0' or j == '':
                    #     j = '1'
                    num = num + '' + j
                else:
                    text += j
                

            product['unit_int'] = num
            product['unit_text'] = text.replace('Approx', '').replace('Size: ', '').strip()

            if product['unit_int'] == '0' or product['unit_int'] == '':
                product['unit_int'] = '1' 


        productsjson.append(product)



new_json = json.dumps(productsjson)
file_obj = open("products_new.json", "w") # write the output to a file
file_obj.write(new_json)