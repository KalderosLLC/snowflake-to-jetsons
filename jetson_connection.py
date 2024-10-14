from calendar import c
from sqlalchemy import create_engine, MetaData, Table, insert, select, func
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy.sql import text
import logging
logger = logging.getLogger(__name__)


class JetsonDatasource:
    def __init__(self, sql_server_engine, jetson_user_id):
        self.sql_server_engine = sql_server_engine
        self.jetson_user_id = jetson_user_id

    def get_latest_covered_entity_id(self):
        metadata = MetaData()
        covered_entity_table = Table('coveredentity', metadata, autoload_with=self.sql_server_engine, schema='hrsa')

        with self.sql_server_engine.connect() as connection:
            stmt = select(func.max(covered_entity_table.c.ID))
            result = connection.execute(stmt).scalar()
            return result if result is not None else 0  # Return 0 if no records found

    def insert_covered_entities(self, covered_entities_df):
        latest_id = self.get_latest_covered_entity_id()
        covered_entities_df['ID'] = covered_entities_df.index + 1
        covered_entities_df['ID'] = covered_entities_df['ID'].apply(lambda x: latest_id + x)

        covered_entities_df['grantNumber'] = None
        covered_entities_df['secondZip'] = None
        
        # Add lastUpdatedDate column with current date and time
        current_datetime = datetime.now()
        covered_entities_df['lastUpdatedDate'] = current_datetime
        # Get the columns of the actual table
        metadata = MetaData()
        covered_entity_table = Table('coveredentity', metadata, autoload_with=self.sql_server_engine, schema='hrsa')

        # Ensure DataFrame columns match the table columns
        table_columns = [column.name for column in covered_entity_table.columns]
        covered_entities_df = covered_entities_df[table_columns]

        records = covered_entities_df.to_dict(orient='records')

        with self.sql_server_engine.connect() as connection:
            # Get existing primary keys from the table
            existing_keys_query = select(covered_entity_table.c.id340B)
            existing_keys_result = connection.execute(existing_keys_query)
            existing_keys = {row[0] for row in existing_keys_result}

            # Separate records into new and duplicates
            new_records = []
            duplicate_records = []

            for record in records:
                key = record['id340B']
                if key in existing_keys:
                    duplicate_records.append(key)
                else:
                    new_records.append(record)
            if duplicate_records:
                print(f"The following keys already exist and were not inserted: {duplicate_records}")
            # Insert new records in batches
            if new_records:
                try:
                    # Prepare the INSERT statement
                    columns = ', '.join(table_columns)
                    placeholders = ', '.join([':' + col for col in table_columns])
                    insert_query = text(f"INSERT INTO hrsa.coveredentity ({columns}) VALUES ({placeholders})")
                    
                    # Insert records in smaller batches
                    batch_size = 1000  # Adjust this value as needed
                    for i in range(0, len(new_records), batch_size):
                        batch = new_records[i:i+batch_size]
                        connection.execute(insert_query, batch)
                        connection.commit()
                        print(f"Inserted batch {i//batch_size + 1} of {len(new_records)//batch_size + 1}")
                    
                    print(f"Successfully inserted {len(new_records)} records into the covered entity table.")
                    return covered_entities_df
                except SQLAlchemyError as e:
                    error_message = str(e.orig)
                    print(f"An error occurred during insertion: {error_message}")
                    return None
            else:
                print("No new records to insert.")
                return covered_entities_df

    def insert_covered_entity_identifiers(self, covered_entity_identifiers_df):
        metadata = MetaData()
        covered_entity_table = Table('coveredentity', metadata, autoload_with=self.sql_server_engine, schema='hrsa')
        covered_entity_identifier_table = Table('coveredentityidentifier', metadata, autoload_with=self.sql_server_engine, schema='hrsa')

        with self.sql_server_engine.connect() as connection:
            try:
                # Get the associated covered entity id for each identifier
                ce_id_query = select(covered_entity_table.c.ID, covered_entity_table.c.id340B)
                ce_id_result = connection.execute(ce_id_query).fetchall()
                ce_id_map = {row.id340B: row.ID for row in ce_id_result}
                covered_entity_identifiers_df['coveredEntityKeyId'] = covered_entity_identifiers_df['id340B'].map(ce_id_map)
                
                # Get the latest covered entity identifier id
                max_id_query = select(func.max(covered_entity_identifier_table.c.id))
                max_id = connection.execute(max_id_query).scalar() or 0
                covered_entity_identifiers_df['id'] = range(max_id + 1, max_id + 1 + len(covered_entity_identifiers_df))

                # Add additional columns
                current_datetime = datetime.now()
                covered_entity_identifiers_df['userCreated'] = False
                covered_entity_identifiers_df['lastUpdatedDate'] = current_datetime
                covered_entity_identifiers_df['activeFlag'] = True
                records = covered_entity_identifiers_df.to_dict(orient='records')

                # stmt = insert(covered_entity_identifier_table).values(records)
                # connection.execute(stmt)
                # connection.commit()
                # print(f"Successfully inserted {len(records)} covered entity identifier records.")
            except SQLAlchemyError as e:
                print(f"An error occurred while inserting covered entity identifiers: {e}")
                return None


    def insert_ce_parents(self, ce_parents_df):
        metadata = MetaData()
        ce_parent_table = Table('ceparentchild', metadata, autoload_with=self.sql_server_engine, schema='hrsa')
        covered_entity_table = Table('coveredentity', metadata, autoload_with=self.sql_server_engine, schema='hrsa')

        with self.sql_server_engine.connect() as connection:
            try:
                # Get the ID for the parent and child CEs if they exist in the coveredEntity table
                ce_id_query = select(covered_entity_table.c.ID, covered_entity_table.c.id340B)
                ce_id_result = connection.execute(ce_id_query).fetchall()
                ce_id_map = {row.id340B: row.ID for row in ce_id_result}

                ce_parents_df['CEKeyIDParent'] = ce_parents_df['parentId340B'].map(ce_id_map)
                ce_parents_df['CEKeyIDChild'] = ce_parents_df['id340B'].map(ce_id_map)

                # Filter out rows where either CEKeyIDParent or CEKeyIDChild is missing
                ce_parents_df = ce_parents_df.dropna(subset=['CEKeyIDParent', 'CEKeyIDChild'])

                # Get existing parent-child relationships
                existing_relationships_query = select(ce_parent_table.c.CEKeyIDParent, ce_parent_table.c.CEKeyIDChild)
                existing_relationships_result = connection.execute(existing_relationships_query)
                existing_relationships = set((row.CEKeyIDParent, row.CEKeyIDChild) for row in existing_relationships_result)

                # Prepare records for insertion, excluding duplicates
                new_records = []
                duplicate_records = []
                for _, row in ce_parents_df.iterrows():
                    relationship = (row['CEKeyIDParent'], row['CEKeyIDChild'])
                    if relationship not in existing_relationships:
                        new_records.append(relationship)
                    else:
                        duplicate_records.append(relationship)

                if duplicate_records:
                    logger.info(f"Found {len(duplicate_records)} duplicate CE parent-child relationships (not inserted)")

                if new_records:
                    try:
                        # Prepare the INSERT statement
                        insert_query = text("INSERT INTO hrsa.ceparentchild (CEKeyIDParent, CEKeyIDChild) VALUES (:CEKeyIDParent, :CEKeyIDChild)")
                        
                        # Insert records in smaller batches
                        batch_size = 1000  # Adjust this value as needed
                        for i in range(0, len(new_records), batch_size):
                            batch = [{'CEKeyIDParent': parent, 'CEKeyIDChild': child} for parent, child in new_records[i:i+batch_size]]
                            connection.execute(insert_query, batch)
                            connection.commit()
                            logger.info(f"Inserted batch {i//batch_size + 1} of {len(new_records)//batch_size + 1}")
                        
                        logger.info(f"Successfully inserted {len(new_records)} new records into the ce parent table.")
                        return ce_parents_df
                    except SQLAlchemyError as e:
                        error_message = str(e.orig)
                        logger.error(f"An error occurred during insertion: {error_message}")
                        return None
                else:
                    logger.info("No new records to insert.")
                    return ce_parents_df

            except SQLAlchemyError as e:
                logger.error(f"An error occurred while preparing ce parents data: {e}")
                return None
