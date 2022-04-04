__author__ = "Gabs the CSE"
__copyright__ = "N/A"
__credits__ = ["Gabriel Cerioni"]
__license__ = "GPL"
__version__ = "0.0.1"
__maintainer__ = "Gabriel Cerioni"
__email__ = "gabriel.cerioni@harness.io"
__status__ = "Brainstorming Phase - DEV/STG/PRD is not applicable"

import os
import csv
import logging

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

# from gql.transport.requests import log as requests_logger

# Configs (if this gets bigger, I'll provide a config file... or even Hashicorp Vault)
# optional - logging.basicConfig(filename='gabs_graphql.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
# this is not working anymore - requests_logger.setLevel(logging.WARNING)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

API_KEY = os.environ.get('HARNESS_GRAPHQL_API_KEY')
HARNESS_ACCOUNT = os.environ.get('HARNESS_ACCOUNT')
API_ENDPOINT = "https://app.harness.io/gateway/api/graphql?accountId={0}".format(HARNESS_ACCOUNT)
# OUTPUT_CSV_NAME_CONST = "temp_instanceStats_parsed.csv"
OUTPUT_CSV_NAME_CONST = os.environ.get('HARNESS_GQL_CSV_NAME')


def generic_graphql_query(query):
    req_headers = {
        'x-api-key': API_KEY
    }

    _transport = RequestsHTTPTransport(
        url=API_ENDPOINT,
        headers=req_headers,
        use_json=True,
    )

    # Create a GraphQL client using the defined transport
    client = Client(transport=_transport, fetch_schema_from_transport=True)

    # Provide a GraphQL query
    generic_query = gql(query)

    # Execute the query on the transport
    result = client.execute(generic_query)
    return result


def get_harness_account_applications():
    offset = 0
    has_more = True
    total_application_list = []

    while has_more:
        query = '''{
        applications(limit: 100, offset: ''' + str(offset) + ''') {
            pageInfo {
                total
                limit
                hasMore
                offset
            }
            nodes {
                name
                id
            }
        }
        }'''

        generic_query_result = generic_graphql_query(query)
        loop_user_list = generic_query_result["applications"]["nodes"]
        total_application_list.extend(loop_user_list)

        #total = generic_query_result["users"]["pageInfo"]["total"]
        has_more = bool(generic_query_result["applications"]["pageInfo"]["hasMore"])

        if has_more:
            offset = offset + 100

    return total_application_list



def parse_result_to_csv(gql_resultset):

    with open(OUTPUT_CSV_NAME_CONST, 'w', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(output_file, fieldnames=gql_resultset[0].keys())
        fc.writeheader()
        fc.writerows(gql_resultset)

    return (gql_resultset)


if __name__ == '__main__':
    logging.info("Starting the Program...")

    logging.info("Retrieving your current instanceStats GraphQL Query result set...")
    result_from_query = get_harness_account_applications()
    print(result_from_query)
    logging.info("Done!")

    logging.info("Expanding all rows from the nested dict - and then putting it on the CSV: {0}".format(OUTPUT_CSV_NAME_CONST))
    parsed_result_set = parse_result_to_csv(result_from_query)
    logging.info("Done! Outputting the list content here:")
    print(parsed_result_set)

    logging.info("Program Exited! Have a nice day!")
