import requests
import logging
import time
from _datetime import datetime
import gw_master
TOKEN = 'Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9idXMiLCJleHAiOjE5ODA0MTI0NDUsImlhdCI6MTY2NTA1MjQ0NSwiYXV0aG9yaXRpZXMiOltdfQ.c8GRMHOGjdaYHFRq6mM7OK3qEKlwjRto5BjyXPB_zMfNNSdqtivDm5DCQPBy1f8JsrAwbGBFHd78f9rJcqKCiw'
cmd_id = "%d" % round(time.time())
BASE_URL = 'https://rf-adapter-prod.adanielectricity.com:443'


def restartGW(Gw_id):
    try:
        url = BASE_URL + "/command/restartGateway"
        headers = {"Authorization": TOKEN}
        params = {
            'commandId': cmd_id,
            'gwId': Gw_id
        }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def send_otap_command(basic_auth, otap_type, gw_id, version):
    try:

        url = BASE_URL + '/config/gwOtap'
        params = {'otapTypes': otap_type,
                  'gwIds': gw_id,
                  'version': version}
        header = {"Authorization": basic_auth}

        response = requests.post(url, params=params, headers=header)

        logging.info(response.url)
        logging.info(response)

        if response.status_code == 200:
            file = open('Command_sent_2.0.1.txt', 'a')
            file.write(str(datetime.now()) + " : " + gw_id + ": " + response.url + ":  " + str(response) + "\n")
            file.close()

            res = response.text
            logging.info(res)
            return res

        else:
            logging.info(response)
            return None

    except requests.exceptions.HTTPError as error:
        logging.error(error)
        return None


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    Otap_count_1 = 1



    for gw in gw_master.gwId_update:
        print(Otap_count_1, 'otap', gw)
        logging.info(gw)
        send_otap_command(
            basic_auth=TOKEN,
            otap_type='MAIN_SERVICE', gw_id=gw,
            version='V2.0.1')
        if Otap_count_1 % 5 == 0:
            # break
            time.sleep(60)

        Otap_count_1 +=1

    # logging.info("861261056663251")
    # send_otap_command(
    #     basic_auth=TOKEN,
    #     otap_type='MAIN_SERVICE', gw_id="861261056663251",
    #     version='1.1.4')

