import argparse
import logging
import yaml
import consul

# Logger configuration
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(name)s :: %(message)s')

# stream logging
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
LOGGER.addHandler(stream_handler)

def create_update_key(k, v, host_url):
    consul_client = consul.Consul(host=host_url)
    consul_client.kv.put(k[1:], str(v))
    LOGGER.info("value for Key: " + k + " updated!")
    return


def is_homogeneous_list(list_data: list):
    res = True
    type_of = type(list_data[0])
    for ele in list_data:
        if not isinstance(ele, type_of):
            res = False
            break
    return res

def yml_parser(data, key, host_url):
    # Use a breakpoint in the code line below to debug your script.
    # print(key, data, sep= " ")
    if data is [] or data is "" or data is {}:
        return
    if isinstance(data, int) or isinstance(data, str) or isinstance(data, float):
        # call update key
        create_update_key(key, data, host_url)
    if isinstance(data, list):
        res = is_homogeneous_list(data)
        # print(res, type(data[0]), sep=" ")
        if res and isinstance(data[0], dict) or isinstance(data[0], list):
            for i in data:
                yml_parser(data=i, key=key, host_url=host_url)
        elif res:
            create_update_key(key, data, host_url)
        elif res is False:
            temp_list = []
            for i in data:
                if isinstance(i, dict) or isinstance(i, list):
                    yml_parser(data=i, key=key, host_url=host_url)
                else:
                    temp_list.append(i)
            if temp_list is not []:
                create_update_key(key, temp_list, host_url)
    if isinstance(data, dict):
        for k, v in data.items():
            yml_parser(data=v, key=key+"/"+k, host_url=host_url)




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--yaml_file', dest='yaml_file', required=True, help="name of yaml file")
    parser.add_argument('-u', '--host_url', dest='host_url', required=True,
                        help="Host url where key/value pair will get store.")
    args = parser.parse_args()

    YAML_FILE = args.yaml_file
    HOST_URL = args.host_url

    with open(YAML_FILE, "r") as stream:
        try:
            LOGGER.info("Fetching Key-Value pair from: " + YAML_FILE)
            LOGGER.info("Updating Key-Value pair on host: " + HOST_URL)
            yml_parser(yaml.safe_load(stream), "", host_url=HOST_URL)
        except yaml.YAMLError as exc:
            print(exc)



