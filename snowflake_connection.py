from xml.etree.ElementTree import QName
import pandas

class SnowflakeDatasource:
    def __init__(self, snowflake_conn, snowflake_database):
        self.snowflake_conn = snowflake_conn
        self.snowflake_database = snowflake_database

    def clean_covered_entities(self, covered_entities_df):
        # Convert to lowercase
        covered_entities_df.columns = covered_entities_df.columns.str.lower()
        
        # Rename columns
        column_mapping = {
            'ce_340b_id': 'id340B',
            'address_zip': 'zip',
            'address_city': 'city',
            'medicare_provider_number': 'medicareProviderNumber',
            'address_street_2': 'address2',
            'address_street_1': 'address1',
            'covered_entity_type': 'entityType',
            'entity_subdivision_name': 'entitySubDivisionName',
            'address_state': 'st',
            'covered_entity_name': 'entityName'
        } 

        # Rename columns
        covered_entities_df = covered_entities_df.rename(columns=column_mapping)
        # Select only the columns that are being remapped
        columns_to_keep = list(column_mapping.values())
        covered_entities_df = covered_entities_df[columns_to_keep]
        return covered_entities_df

    def clean_covered_entity_identifiers(self, covered_entity_identifiers_df):
        # Convert to lowercase
        covered_entity_identifiers_df.columns = covered_entity_identifiers_df.columns.str.lower()
        
        # Rename columns
        column_mapping = {
            'identifier_field_name': 'identifierType',
            'crosswalked_identifier_field_value': 'identifier',
            'ce_340b_id': 'id340B'
        }

        # Rename columns
        covered_entity_identifiers_df = covered_entity_identifiers_df.rename(columns=column_mapping)
        
        # Select only the columns that are being remapped
        columns_to_keep = list(column_mapping.values()) 
        covered_entity_identifiers_df = covered_entity_identifiers_df[columns_to_keep]
        return covered_entity_identifiers_df
    
    def clean_contract_pharmacies(self, contract_pharmacies_df):
        # Define column mapping
        column_mapping = {
            'value': 'id',
            'PHARMACY_NAME': 'HRSAPHARMACYNAME',
            'ADDRESS_STREET_1': 'HRSAADDRESS1',
            'address_city': 'city',
            'address_state': 'st',
            'address_zip': 'zip',
            'address_zip4': 'secondZip',
            'contract_pharmacy_ncpdp': 'ncpdp'
        }

        # Rename columns
        contract_pharmacies_df = contract_pharmacies_df.rename(columns=column_mapping)
        
        # Select only the columns that are being remapped
        columns_to_keep = list(column_mapping.values()) 
        contract_pharmacies_df = contract_pharmacies_df[columns_to_keep]
        return contract_pharmacies_df

    def clean_ce_parents(self, ce_parents_df):
        # Convert to lowercase
        ce_parents_df.columns = ce_parents_df.columns.str.lower()
        
        # Rename columns
        column_mapping = {
            'ce_340b_id': 'id340B',
            'parent_ce_340b_id': 'parentId340B',
            'covered_entity_key_id': 'CEKeyIDChild'
        }

        # Rename columns
        ce_parents_df = ce_parents_df.rename(columns=column_mapping)
        return ce_parents_df
    

    # ------------------------------

    def get_covered_entities(self):
        covered_entities_query = f"""
            select r66.*
            from {self.snowflake_database}.silver.mart_covered_entities r66
            left join fivetran_database.kalderos_web_hrsa.coveredentity kprod
                on kprod.id  = r66.covered_entity_key_id
            where kprod.id is null
        """

        snowflake_cursor = self.snowflake_conn.cursor()
        snowflake_cursor.execute(covered_entities_query)
        snowflake_df = snowflake_cursor.fetch_pandas_all()
        snowflake_df = self.clean_covered_entities(snowflake_df)
        return snowflake_df
    
    def get_covered_entity_identifiers(self):
        covered_entity_identifiers_query = f"""
            select r66.*
            from {self.snowflake_database}.silver.mart_covered_entities_identifier_crosswalk as r66
            inner join {self.snowflake_database}.silver.mart_covered_entities as ce_mart
                on ce_mart.ce_340b_id = r66.ce_340b_id
            left join fivetran_database.kalderos_web_hrsa.coveredentityidentifier as kprod
                on kprod.coveredentitykeyid = ce_mart.covered_entity_key_id
                and kprod.identifier = r66.crosswalked_identifier_field_value
            where kprod.coveredentitykeyid is null
            and r66.identifier_field_name != 'medicaid_number'
        """

        snowflake_cursor = self.snowflake_conn.cursor()
        snowflake_cursor.execute(covered_entity_identifiers_query)
        snowflake_df = snowflake_cursor.fetch_pandas_all()
        snowflake_df = self.clean_covered_entity_identifiers(snowflake_df)
        return snowflake_df
    

    def get_contract_pharmacies(self):
        contract_pharmacies_query = f"""
            SELECT r66.*
            FROM {self.snowflake_database}.silver.mart_contract_pharmacies r66
            LEFT JOIN fivetran_database.kalderos_web_hrsa.contractpharmacy kprod
                ON kprod.id = r66.contract_pharmacy_key_id
            WHERE kprod.id IS NULL;
        """

        snowflake_cursor = self.snowflake_conn.cursor()
        snowflake_cursor.execute(contract_pharmacies_query)
        snowflake_df = snowflake_cursor.fetch_pandas_all()
        snowflake_df = self.clean_contract_pharmacies(snowflake_df)
        return snowflake_df
    
    def get_ce_parents(self):
        ce_parents_query = f"""
            select r66.ce_340b_id, r66.parent_ce_340b_id, r66.covered_entity_key_id
            from {self.snowflake_database}.silver.mart_covered_entities as r66
            left join fivetran_database.kalderos_web_hrsa.ceparentchild as kprod_pce
                on kprod_pce.cekeyidparent = r66.covered_entity_key_id
            where kprod_pce.id is null
            and source ='springfield'
        """

        snowflake_cursor = self.snowflake_conn.cursor()
        snowflake_cursor.execute(ce_parents_query)
        snowflake_df = snowflake_cursor.fetch_pandas_all()
        snowflake_df = self.clean_ce_parents(snowflake_df)
        return snowflake_df
 