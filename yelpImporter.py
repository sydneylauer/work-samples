import os
from dotenv import load_dotenv
load_dotenv()
logzio_token = os.getenv("LOGZIO_TOKEN")
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from logger_local.LoggerLocal import logger_local as locallgr
from circles_importer import importer 
from circles_local_database_python import database
import time
import json

def connect():
  database_conn = database.database()
  db = database_conn.connect_to_database()
  return db

class YelpImporter: 

  def get_data(self, business_type, location):
    locallgr.init("Fetching business data from Yelp GraphQL", object = {"component_id": 156, "component_name":"Local yelp importer"})

    YELP_API_ENDPOINT = 'https://api.yelp.com/v3/graphql'

    query = gql('''
      query ($term: String!, $location: String!, $limit: Int!, $offset: Int!) {
        search(term: $term, location: $location, limit: $limit, offset: $offset) {
          business {
            name
            rating
            location {
              address1
              city
              state
              country
              postal_code
            }
            phone
            photos
            coordinates {
              latitude
              longitude
            }
            hours {
              hours_type
              is_open_now
              open {
                day
                is_overnight
                end
                start
              }
            }
          }
          total
        }
      }
    ''')

    # Define GraphQL transport
    transport = RequestsHTTPTransport(
        url=YELP_API_ENDPOINT,
        headers={'Authorization': f'Bearer {os.getenv("YELP_API_KEY")}'},
        use_json=True,
    )

    # Define GraphQL client
    graphql_client = Client(
        transport=transport,
        fetch_schema_from_transport=False,
    )

    limit = 50
    offset = 0
    data = {"results": []}
    total = float('inf')

    while offset < total and offset < 950:
        try: 
          response = graphql_client.execute(query, variable_values={'term': business_type, 'location': location, 'limit': limit, 'offset': offset})
          for business in response['search']['business']:
              locallgr.info(object = {"Business_dict": business['name']})
              dict = {}
              #reformat dictionary to fit generic template 
              dict["name"] = business["name"]
              coordinates = business["coordinates"]
              dict["location"] = {"coordinates": coordinates, 
                                  "address_local_language": business["location"]["address1"],
                                  "city": business["location"]["city"], 
                                  "country": business["location"]["country"],
                                  "postal_code": business["location"]["postal_code"]
                                },
              dict["phone"] = {"number_original": business["phone"]},
              dict["storage"] = {"path": business["photos"]},
              dict["reaction"] = {"value": business["rating"], "reaction_type": "Rating"},
              dict["operational_hours"] = []
              if len(business["hours"]) > 0:
                for day_dict in business["hours"][0]["open"]:
                  dict["operational_hours"].append({"day_of_week": day_dict["day"], "from": self.reformat_time_string(day_dict["start"]), "until": self.reformat_time_string(day_dict["end"])})

              data["results"].append(dict)

          offset += limit
          total = response['search']['total']
          locallgr.info("Retrieved data for " + str(offset) + " businesses so far", object = {"Total": offset})
        
        except Exception as e:
          locallgr.exception(f"Exception occurred: {str(e)}")
          print (f"Exception occurred: {str(e)}")

    locallgr.end(object = {"Business_total": offset})
    return json.dumps(data)
  
  #TO D0:
  # def insert_profile_and_update_importer(self, conn = connect()):
  # entity_id = profile generic package
  #   my_importer = importer.Importer("Yelp.com GraphQL", 1)
    
  #   my_importer.insert_record_data_source("United States", "Business Profile", entity_id, "https://api.yelp.com/v3/graphql")

  def reformat_time_string(self, input_str):
    hours = input_str[:2]
    minutes = input_str[2:]
    time_format = f"{hours}:{minutes}:00:00"
    return time_format

