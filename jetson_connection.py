from calendar import c
from sqlalchemy import create_engine, MetaData, Table, insert, select, func
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from datetime import datetime, timedelta, timezone


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
        
        # Add lastUpdatedDate column with current date and time
        current_datetime = datetime.now()
        covered_entities_df['lastUpdatedDate'] = current_datetime
        # Get the columns of the actual table
        metadata = MetaData()
        covered_entity_table = Table('coveredentity', metadata, autoload_with=self.sql_server_engine, schema='hrsa')
        records = covered_entities_df.to_dict(orient='records')

        with self.sql_server_engine.connect() as connection:
            try:
                stmt = insert(covered_entity_table).values(records)
                connection.execute(stmt)
                connection.commit()
                print(f"Successfully inserted {len(records)} records into the covered entity table.")
                return covered_entities_df
            except SQLAlchemyError as e:
                print(f"An error occurred: {e}")
                return None

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
                # Get the ID for the parent CE if it exists in the coveredEntity table
                parent_id_query = select(covered_entity_table.c.ID, covered_entity_table.c.id340B)
                parent_id_result = connection.execute(parent_id_query).fetchall()
                parent_id_map = {row.id340B: row.ID for row in parent_id_result}
                ce_parents_df['CEKeyIDParent'] = ce_parents_df['parentId340B'].map(parent_id_map)

                # Update ceKeyIDChild by checking the coveredEntity table
                child_id_query = select(covered_entity_table.c.ID, covered_entity_table.c.id340B)
                child_id_result = connection.execute(child_id_query).fetchall()
                child_id_map = {row.id340B: row.ID for row in child_id_result}
                ce_parents_df['CEKeyIDChild'] = ce_parents_df['id340B'].map(child_id_map)

                print(ce_parents_df.head())
                # Filter out rows where either ceKeyIDParent or ceKeyIDChild is missing
                ce_parents_df = ce_parents_df.dropna(subset=['CEKeyIDParent', 'CEKeyIDChild'])

                # Prepare records for insertion
                records = ce_parents_df[['CEKeyIDParent', 'CEKeyIDChild']].to_dict(orient='records')

                # Insert records into the ce_parent_table
                stmt = insert(ce_parent_table).values(records)
                connection.execute(stmt)
                connection.commit()
                print(f"Successfully inserted {len(records)} records into the ce parent table.")
                return ce_parents_df
            except SQLAlchemyError as e:
                print(f"An error occurred while inserting ce parents: {e}")
                return None


