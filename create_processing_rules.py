import requests
import pandas as pd
from vault.catalog import Catalog
import warnings; warnings.simplefilter('ignore')
import os
import json
import plotly.offline as offline
pd.core.common.is_list_like = pd.api.types.is_list_like
offline.init_notebook_mode(connected=True)


ENV = 'prod'
TENANT = 'astdevvault'

URL_TOKEN = 'https://login.microsoftonline.com/2807b866-c9a5-4778-84c9-27c31366d350/oauth2/token'
URL_RULES = 'https://apps.prod.az.eagleinvsys.com:8443/api/vault/eds/eagle/v3/inflow'
URL_RESORCES = 'https://apps.prod.az.eagleinvsys.com:8443/api/vault/metadata/v2/metadata/resources'

startswith_flag = False
list_flag = False


def read_token():
    "Reads and returns a token from a file."

    f = open("token.txt", "r+")
    mytoken = f.read()
    f.close()
    return mytoken


def check_feeds_configured(mytoken, feed_name, URL_RESORCES):
    "Checks if there is an existing processing rule for the feed."

    header = {'Accept': 'application/json',
              'Content-Type': 'application/json',
              "Authorization": "Bearer %s"% mytoken,
              'x-eagle-context': TENANT}
    parameters = {"resourceversion": "1.0.1"}
    URL_RESORCES = URL_RESORCES + '/' + feed_name
    response = requests.get(URL_RESORCES, headers=header, params=parameters)
    return response.status_code


def get_models_to_set(models_list, prefix):
    "Selects models in tenant matching a request list or starting with a specified prefix depending on the flag."

    catalog = Catalog(ENV, TENANT, verbose=False)
    full_list = catalog.get_model_list()
    if len(full_list) > 0:
        if startswith_flag and prefix:
            to_set_list = [x for x in full_list if (x['name'].startswith(prefix))]
        elif list_flag:
            to_set_list = [x for x in full_list if (x['name'] in models_list)]
    else:
        print(f"No models found in {TENANT} tenant")
    return to_set_list


def get_feeds_to_set(mytoken, to_set_list):
    "Gets a dictionary of feeds and corresponding SF tables for which processing rule generation is required."

    feed_dict = {}
    if len(to_set_list) > 0:
        for el in to_set_list:
            feed_name = el.get('feedType')
            vendor = len(el.get('vendor'))
            system = len(el.get('feedSystem'))
            name = el.get('name').lower()
            table_name = TENANT.upper() + '.' + name.upper()
            if feed_name is None:
                to_remove = vendor + system + 2
                feed_name = name[to_remove:]
            else:
                feed_name = feed_name
            if (check_feeds_configured(mytoken, feed_name, URL_RESORCES)) == 404:
                feed_dict[feed_name] = table_name
            elif (check_feeds_configured(mytoken, feed_name, URL_RESORCES)) == 200:
                f = open("models_with_existing_rules.txt", "a")
                f.writelines(name + ', ')
                f.close()
            elif (check_feeds_configured(mytoken, feed_name, URL_RESORCES)) == 401:
                print("You are not authorized.")
    else:
        print('For these models, you do not need to generate processing rules.')
    return feed_dict


def find(feed_name):
    "Returns the file name corresponding to the feed name and found in the 'data' directory."

    path = os.getcwd()
    if os.path.exists('data'):
        for root, dirs, files in os.walk(path + '/data'):
            if len(files) > 0:
                for name in files:
                    if feed_name in name.lower():
                        return name
            else:
                print("There are no files in 'data' directory.")


def create_processing_rules(mytoken, feed_dict):
    "Creates proccesing rules."

    headers = {
        'accept': 'application/json',
        'X-Eagle-Context': TENANT,
        "Authorization": "Bearer %s"% mytoken,
    }

    instructions = {
        "ResourceName": "feed_name",
        "dbprovider": "snowflake",
        "schemadriftmode": "2",
        "useTablePath": "table_name",
        "preserveColumnNameCase": "Y"
    }

    for feed_name, table_name in feed_dict.items():
        instructions["ResourceName"] = feed_name
        instructions["useTablePath"] = table_name
        new_ins = json.dumps(instructions)
        filename = find(feed_name)
        files = {
            "file": (filename, open('data/' + filename, 'rb')),
        }
        response = requests.post(URL_RULES, headers=headers, data={'instructions': new_ins}, files=files)
        if response.status_code == 200:
            f = open("success.txt", "a")
            f.writelines(feed_name + ', ')
            f.close()


def main():

    mytoken = read_token()
    to_set_list = get_models_to_set(models_list, prefix)
    feed_dict = get_feeds_to_set(mytoken, to_set_list)
    if len(feed_dict) > 0:
        create_processing_rules(mytoken, feed_dict)
    else:
        print("For these models, you do not need to generate processing rules.")


if __name__ == "__main__":

    list_flag = True
    prefix = 'BNYM_ZTH_ETA_'
    models_list = ['BNYM_ADU_MO_ACCOUNT_DIMENSIONS', 'BNYM_ADU_MO_ACCOUNT_GROUPS',
                   'BNYM_ADU_MO_ACCOUNT_XREFERENCES', 'BNYM_ADU_MO_FX_RATES',
                   'BNYM_ADU_MO_INSTRUMENT_DIMENSIONS', 'BNYM_ADU_MO_TAXLOTS',
                   'BNYM_ADU_MO_VALUATION_PRICES', 'BNYM_ADU_MO_ACCOUNT_THIRD_PARTIES'
                   ]

    main()
