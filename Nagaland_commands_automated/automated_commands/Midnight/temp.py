base_url = 'http://216.48.180.61:9999'
def auth():
    try:
        url = base_url + '/auth/login'
        credential = {
            "password": "probus@123",
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