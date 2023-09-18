from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()
#os.environ["DEBUG"] = "1"
logzio_token = os.getenv("LOGZIO_TOKEN")
from circles_local_database_python import database
import re
import json
from logger_local.LoggerLocal import logger_local
import time 
import mysql
from CirclesNumberGenerator.number_generator import NumberGenerator as num_gen


def db_connection():
    database_conn = database.database()
    db = database_conn.connect_to_database()
    return db


class TextBlocks:
    def __init__(self, date, update, created_user_id = 1, updated_user_id = 1):
        self.date = date
        self.update = update
        self.created_user_id = created_user_id
        self.updated_user_id = updated_user_id

    def get_block_fields(self, block_type_id):
        logger_local.start("Getting regex and field_id from block_id ...")

        conn = db_connection()
        if conn:
            cursor = conn.cursor()

            cursor.execute("SELECT regex, field_id FROM field.block_type_field_table WHERE block_type_id = %s OR block_type_id IS NULL", (block_type_id,))
            block_fields = dict((regex, field_id) for regex, field_id in cursor.fetchall())

            cursor.close()
            conn.close()
            logger_local.end("Regex and field ids retrieved", object={'block_fields': block_fields})
            return block_fields

        logger_local.error("Error connecting to the database")

    def get_fields(self):
        logger_local.start("Getting field ids and names ...")

        conn = db_connection()
        if conn:
            cursor = conn.cursor()

            cursor.execute("SELECT id, name FROM field.field_table")
            fields = dict((id, name) for id, name in cursor.fetchall())

            cursor.close()
            conn.close()
            logger_local.end("Field names and ids retrieved", object={'fields': fields})
            return fields

        logger_local.error("Error connecting to the database")

    def get_block_type_ids_regex(self):
        logger_local.start("Getting block type ids and names ...")

        conn = db_connection()
        if conn:
            cursor = conn.cursor()

            cursor.execute("SELECT text_block_type_id, regex FROM text_block_type.text_block_type_regex_table")
            block_types = dict((regex, id) for id, regex in cursor.fetchall())

            cursor.close()
            conn.close()
            logger_local.end("Block types retrieved", object={'block_types': block_types})
            return block_types

        logger_local.error("Error connecting to the database")

    def get_block_types(self):
        logger_local.start("Getting block type ids and names ...")    

        conn = db_connection()
        if conn: 
            cursor = conn.cursor()

        cursor.execute("SELECT name, id FROM text_block_type.text_block_type_ml_table")
        block_types = dict ((id, name) for name, id in cursor.fetchall())

        cursor.close()
        conn.close()
        logger_local.end("Block types retrieved", object = { 'block_types': block_types})    
        return block_types

        logger_local.error("Error connecting to the database")

    def get_text_block_ids_types(self):
        logger_local.start("Getting text blocks from text_block_table ...")

        conn = db_connection()
        if conn:
            cursor = conn.cursor()

            cursor.execute("SELECT id, text_block_type_id, text_without_empty_lines, text FROM text_block.text_block_table")
            text_block_ids_types = {}
            for id, type_id, text_without_empty_lines, text in cursor.fetchall():
                if text_without_empty_lines:
                    text_block_ids_types[id] = (type_id, text_without_empty_lines)
                else:
                    text_block_ids_types[id] = (type_id, text)

            cursor.close()
            conn.close()
            logger_local.end("Text blocks retrieved", object={'text_blocks_ids_types': text_block_ids_types})
            return text_block_ids_types

    def process_text_block_since_date(self):
        conn = db_connection()
        if conn:
            cursor = conn.cursor()

            cursor.execute("SELECT id FROM text_block.text_block_table WHERE updated_timestamp >= %s", (self.date,))
            text_block_ids = [id[0] for id in cursor.fetchall()]

            cursor.close()
            conn.close()

            for text_block_id in text_block_ids:
                self.process_text_block(text_block_id)

    def process_text_block(self, text_block_id):
        conn = db_connection()
        if not conn:
            logger_local.error("Error connecting to the database")
            return
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT text_without_empty_lines, text, text_block_type_id, profile_id FROM text_block.text_block_table WHERE id = '%s'", (text_block_id,))
            result = cursor.fetchone()
            if result[0]:
                (text, text_block_type_id, profile_id) = (result[0], result[2], result[3])
            else:
                (text, text_block_type_id, profile_id) = (result[1], result[2], result[3])

            # reformat text
            text = text.replace("\n", " ")

            # update block_type if it does not exist
            if text_block_type_id is None:
                self.identify_and_update_text_block_type(text_block_id, text)

            # get dictionary of field ids and names
            fields = self.get_fields()
            if fields is None:
                return "Error retreieving fields dictionary"

            # get block id and corresponding regex/fields
            block_fields = self.get_block_fields(text_block_type_id)
            if block_fields is None:
                return "Error retreieving block fields dictionary"

            # For debugging purposes: create a dictionary with all the fields extracted from a specific Text Block
            fields_dict = {}

            # Per each relevant field (this text_block_type or null/zero text_block_type)
            for regex in block_fields:
                if regex:
                    # check the regex is valid
                    try:
                        re.compile(regex)
                        matches = re.findall(regex, text)

                        field_id = block_fields[regex]
                        field = fields[field_id]

                        if len(matches) > 0: 
                            fields_dict[field] = matches

                            for match in matches:
                                cursor.execute("SELECT table_id, database_field_name, database_sub_field_name, database_sub_field_value, processing_id, processing_database_field_name from field.field_table WHERE id = %s", (field_id,))
                                result = cursor.fetchone()
                                (table_id, database_field_name, database_sub_field_name, database_sub_field_value, processing_id, processing_database_field_name) = (result[0], result[1], result[2], result[3], result[4], result[5])
                                try: 
                                    #TO DO: process fields with _original
                                    #processed_value = self.process_field(processing_id, match)

                                    cursor.execute("SELECT `schema`, table_name, profile_mapping_table_id FROM database.table_definition_table WHERE id = %s", (table_id,))
                                    result = cursor.fetchone()
                                    (schema, table_name, profile_mapping_table_id) = (result[0], result[1], result[2])

                                    cursor.execute("SELECT `schema`, table_name FROM database.table_definition_table WHERE id = %s", (profile_mapping_table_id,))
                                    result = cursor.fetchone()
                                    if result is not None:
                                        (profile_mapping_table_schema, profile_mapping_table_name) = (result[0], result[1]) 

                                    # Create SQL UPDATE Statement to update the relevant tables based on field_table.table_id, field_table.database_field_name
                                    sql = ""
                                    if profile_id and profile_mapping_table_id:
                                        sql = "SELECT " + schema +  "_id FROM %s.%s WHERE profile_id = %s" % (profile_mapping_table_schema, profile_mapping_table_name, profile_id)
                                        cursor.execute(sql)
                                        mapping_id = cursor.fetchone()[0]

                                        sql = "SELECT %s FROM %s.%s WHERE id = %s" % (database_field_name, schema, table_name, mapping_id)
                                        cursor.execute(sql)
                                        field_old = cursor.fetchone()
                                        if field_old is not None:
                                            sql = "UPDATE %s.%s SET %s = '%s' WHERE id = %s" % (schema, table_name, database_field_name, match, mapping_id)
                                            if database_sub_field_name and database_sub_field_value:
                                                sql = "UPDATE %s.%s SET %s = '%s', %s = '%s' WHERE id = %s" % (schema, table_name, database_field_name, match, database_sub_field_name, database_sub_field_value, mapping_id)
                                            print (sql)
                                            cursor.execute(sql)
                                            conn.commit()
                                            if field_old[0] != match:
                                                self.update_logger_with_old_and_new_field_value(field_id, field_old[0], match)
                                    else:
                                        #Populate the person/profile class for each profile processed 
                                        profile_id = self.create_person_profile(fields_dict)

                                        #insert information from extracted fields
                                        sql = "INSERT IGNORE INTO %s.%s (%s, created_user_id, updated_user_id) VALUES ('%s', %s, %s)" % (schema, table_name, database_field_name, match, self.created_user_id, self.updated_user_id)
                                        if database_sub_field_name and database_sub_field_value:
                                            sql = "INSERT IGNORE INTO %s.%s (%s, %s, created_user_id, updated_user_id) VALUES ('%s', '%s', %s, %s)" % (schema, table_name, database_field_name, database_sub_field_name, match, database_sub_field_value, self.created_user_id, self.updated_user_id)        
                                        logger_local.info(object = {"SQL command executed": sql})
                                        cursor.execute(sql)
                                        print(sql)
                                        mapping_id = cursor.lastrowid
                                        conn.commit()

                                        #update the profile_mapping table
                                        if profile_mapping_table_schema:
                                            sql = "INSERT IGNORE INTO %s.%s (profile_id, %s_id, created_user_id, updated_user_id) VALUES (%s, %s, %s, %s)" % (profile_mapping_table_schema, profile_mapping_table_name, schema, profile_id, mapping_id, self.created_user_id, self.updated_user_id)
                                            if profile_mapping_table_schema == "group_profile":
                                                sql = "INSERT IGNORE INTO %s.%s (profile_id, %s_id, relationship_type_id, created_user_id, updated_user_id) VALUES (%s, %s, %s, %s, %s)" % (profile_mapping_table_schema, profile_mapping_table_name, schema, profile_id, 5, mapping_id, self.created_user_id, self.updated_user_id)
                                            cursor.execute(sql)
                                            print(sql)
                                            conn.commit()

                                except Exception as e:
                                    #print (str(e))
                                    logger_local.exception(object=e)

                    # print error if regex is invalid
                    except re.error as e:
                        print("Regex failed to compile: " + regex + "\n")
                        logger_local.exception(str("Invalid regex: ", regex), object=e)

            # update the text_block_table.fields_extracted_json with fields dictionary
            fields_json = json.dumps(fields_dict)
            sql = "UPDATE text_block.text_block_table SET fields_extracted_json = '{}' WHERE id = {}".format(fields_json, text_block_id)
            print(sql)
            cursor.execute(sql)
            conn.commit()
            logger_local.info(object = {"SQL command executed": sql})

            print ("Fields dictionary for text block id %s: %s\n" % (text_block_id, fields_dict))

        except mysql.connector.errors.DatabaseError as e:
            if "Lock wait timeout exceeded" in str(e):
                logger_local.info("Lock wait timeout exceeded. Retrying UPDATE after a short delay.")
                time.sleep(2)  
                self.process_text_block(text_block_id)  
            else:
                print (str(e))
                logger_local.exception("Database Error", object=e)

        cursor.close()
        conn.close()


    def identify_and_update_text_block_type(self, text_block_id, text):
        text_block_type_id = self.identify_text_block_type(text_block_id, text)
        if self.update == False:
            return text_block_type_id

        conn = db_connection()
        if not conn:
            logger_local.error("Error connecting to the database")
            return
        cursor = conn.cursor()

        # SQL UPDATE text_block.text_block_type
        if text_block_type_id is not None:
            sql = "UPDATE text_block.text_block_table SET text_block_type_id = %s WHERE id = %s" % (text_block_type_id, text_block_id)
            cursor.execute(sql)
            conn.commit()
            logger_local.info(object = {"SQL command executed": sql})

        conn.close()
        return text_block_type_id

    def identify_text_block_type(self, text_block_id, text):
        conn = db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT system_id, system_entity_id FROM text_block_type.text_block_type_table WHERE id = %s", (text_block_id,))
            
            potential_block_type_ids = self.get_block_type_ids_regex()

            try:
                result = cursor.fetchone()
                (system_id, system_entity_id) = (result[0], result[1])
                # filter results with system_id and system_entity if possible
                if system_entity_id:
                    cursor.execute("SELECT regex from text_block_type.text_block_type_table WHERE system_id = %s AND system_entity_id = %s", (system_id, system_entity_id))
                else:
                    cursor.execute("SELECT regex from text_block_type.text_block_type_table WHERE system_id = %s", (system_id,))
                regex_list = [regex for tpl in cursor.fetchall() for regex in tpl]
                cursor.execute("SELECT regex, text_block_type_id FROM text_block_type.text_block_type_regex_table WHERE regex IN regex_list")
                potential_block_type_ids = dict((regex, text_block_type_id) for regex, text_block_type_id in cursor.fetchall())
            except Exception as e:
                pass
                logger_local.exception("No system id for text block", object=e)
        conn.close()

        # classify block_type using regex
        for regex in potential_block_type_ids:
            try:
                re.compile(regex)
                match = re.search(regex, text)
                if match:
                    return potential_block_type_ids[regex]
            except re.error as e:
                print("Regex failed to compile: " + regex + "\n")
                logger_local.exception(str("Invalid regex: ", regex), object=e)

        # if no block type id has been found by this point
        logger_local.info("Unable to identify block_type_id for text block", object={'text_block_id': text_block_id})
        print ("Unable to identify block type")
        return None

    def check_all_text_blocks(self):
        # For all text_blocks
        text_block_ids_types = self.get_text_block_ids_types()
        block_types = self.get_block_types()
        for id in text_block_ids_types:
            existing_block_type = text_block_ids_types[id][0]
            if existing_block_type:
                print("\nOld block type: " + str(existing_block_type) + ", '" + block_types[existing_block_type] + "' for text block " + str(id))
            else:
                print ("Old block type: None")
            text = (text_block_ids_types[id][1]).replace("\n", " ")
            new_block_type = self.identify_and_update_text_block_type(id, text)
            if new_block_type is not None:
                print ("Identified block type: " + str(new_block_type) + " " + block_types[new_block_type])

    def update_logger_with_old_and_new_field_value(self, field_id, field_value_old, field_value_new, conn = db_connection()):
        if not conn:
            logger_local.error("Error connecting to the database")
            return
        
        cursor = conn.cursor()
        sql = "INSERT INTO logger.logger_table (field_id, field_value_old, field_value_new) VALUES ('%s', '%s', '%s')" % (field_id, field_value_old, field_value_new)
        cursor.execute(sql)
        logger_local.info(object = {"SQL command executed": sql})
        conn.commit()
        cursor.close()
        conn.close()

    def create_person_profile(self, fields_dict):
        conn = db_connection()
        if not conn:
            logger_local.error("Error connecting to the database")
            return
        cursor = conn.cursor()

        number = num_gen("person", "person_table").get_random_number()
        time_now = time.time() 
        if "First Name" in fields_dict and "Last Name" in fields_dict:
            first_name = fields_dict["First Name"][0]
            last_name = fields_dict["Last Name"][0]
            sql = "INSERT IGNORE INTO person.person_table (number, first_name, last_name, last_coordinate, created_user_id, updated_user_id, start_timestamp) VALUES (%s, '%s', '%s', POINT(0.0000, 0.0000), %s, %s, %s)" % (number, first_name, last_name, self.created_user_id, self.updated_user_id, time_now)
        elif "Birthday" in fields_dict:
            birthday = fields_dict["Birthday"][0]
            sql = "INSERT IGNORE INTO person.person_table (number, birthday_original, last_coordinate, created_user_id, updated_user_id, start_timestamp) VALUES (%s, '%s', POINT(0.0000, 0.0000), %s, %s, %s)" % (number, birthday, self.created_user_id, self.updated_user_id, time_now)
        else:
            sql = "INSERT IGNORE INTO person.person_table (number, last_coordinate, created_user_id, updated_user_id, start_timestamp) VALUES (%s, POINT(0.0000, 0.0000), %s, %s, %s)" % (number, self.created_user_id, self.updated_user_id, time_now)
        print (sql)
        cursor.execute(sql)
        person_id = cursor.lastrowid
        conn.commit()

        sql = "INSERT IGNORE INTO profile.profile_table(number, person_id, visability_id, created_user_id, updated_user_id) VALUES (%s, %s, 0, %s, %s)" % (number, person_id, self.created_user_id, self.updated_user_id)
        print (sql)
        cursor.execute(sql)
        profile_id = cursor.lastrowid
        conn.commit()
        
        cursor.close()
        conn.close() 

        return profile_id
    
    def process_field(processing_id, match):
        pass
        #if processing_id == 1: #birthday YYYY-MM-DD

        #else if processing_id ==2: #phone

        #return processed_value

def main():
    logger_local.init(object = {'component_id':'143', 'component_name': 'text_block_local_python_package'})
    
    tester = TextBlocks(datetime.strptime("2023-07-20 12:34:56", "%Y-%m-%d %H:%M:%S"), True)

    #tester.check_all_text_blocks()
    tester.process_text_block_since_date()

if __name__ == "__main__":
    main()


