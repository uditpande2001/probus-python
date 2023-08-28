import requests
import logging
from master.master_file import batch_nodes
import csv

base_url = 'https://rf-adapter-prod.adanielectricity.com:443'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def auth():
    try:
        url = base_url + '/auth/login'
        credential = {
            "password": "lAgRGmb8abCVfrBX",
            "userId": "probus"
        }
        response = requests.post(url=url, json=credential)
        logging.info(response.url)

        if response.status_code == 200:
            token = response.text
            logging.info(token)
            return token

        else:
            logging.error(response)
            logging.error(response.text)
            return None
    except requests.exceptions.HTTPError as error:
        logging.error(error)
        return None


res_token = auth()


def nodes_batch(node_list):
    url = base_url + '/config/getNodesBatch'

    nodeIds = node_list
    batch_size = {'batchSize': 1}
    head = {
        'Authorization': res_token
    }
    response = requests.post(url, json=nodeIds, params=batch_size, headers=head)
    if response.status_code == 200:
        # logging.info(response.text)
        return response.json()
    else:
        logging.error(response)
        logging.error(response.text)

def count_elements(arr):
    count = 0
    elements_array = []  # New array to store the elements
    for element in arr:
        if isinstance(element, list):
            nested_count, nested_elements = count_elements(
                element)  # Recursively count elements in nested array
            count += nested_count
            elements_array.extend(nested_elements)
        else:
            count += 1  # Found a single element
            elements_array.append(element)  # Add the element to the new array
    return count, elements_array

# Count the total number of elements and add them to a new array


if __name__ == '__main__':
    print(len(batch_nodes))
    temp = nodes_batch(batch_nodes)
    print(temp)
    num_elements, new_array = count_elements(temp)

    print("Total number of elements:", num_elements)
    print("New array:", new_array)

    file = open('test_batch_file.csv','w', newline='')
    writer = csv.writer(file)
    for nodes in new_array:
        writer.writerow([nodes])

    file.close()
