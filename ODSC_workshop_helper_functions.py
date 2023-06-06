# load the featurebyte SDK
import uuid
import featurebyte as fb
from featurebyte.api.request_column import RequestColumn

# load the datetime module
from datetime import datetime

# load the regular expressions module
import re

def to_python_variable_name(name: str):
    result = name.lower().replace(" ", "_").replace("-", "_")
    if result[0].isdigit():
        result = "digit_" + result
    # replace any non-alphanumeric characters with an underscore
    result = re.sub(r'\W+', '_', result)
    # make the characters lower case
    result = result.lower()
    return result

def is_tutorial_catalog(catalog_name):
    # does the catalog name contain playground? if so, it is not a tutorial catalog
    if catalog_name.lower().find("playground") != -1:
        return False

    # does the catalog name begin with "quick start " or "deep dive "? if so, it is probably a tutorial catalog
    if not (catalog_name.lower().startswith("quick start ") or catalog_name.lower().startswith("deep dive ")):
        return True

    # does the catalog name begin with "healthcare demo " or "credit card demo "? if so, it is probably a tutorial catalog
    if not (catalog_name.lower().startswith("healthcare demo ") or catalog_name.lower().startswith("credit card demo ")):
        return True

    return True

def clean_catalogs(verbose = True):
    # get active catalog
    current_catalog = fb.Catalog.get_active()
    
    cleaned = False
    
    # loop through the catalogs
    for catalog_name in fb.list_catalogs().name:
        if is_tutorial_catalog(catalog_name):
            temp_catalog = fb.Catalog.get(catalog_name)
            
            # get lists of each object type that may need to be removed
            deployments = temp_catalog.list_deployments()
            batch_feature_tables = temp_catalog.list_batch_feature_tables()
            batch_request_tables = temp_catalog.list_batch_request_tables()
            historical_feature_tables = temp_catalog.list_historical_feature_tables()
            observation_tables = temp_catalog.list_observation_tables()
            
            # get a count of existing objects
            num_deployments =0
            for id in deployments.id:
                deployment = temp_catalog.get_deployment_by_id(id)
                if deployment.enabled:
                    num_deployments = num_deployments + 1
            num_batch_feature_tables = batch_feature_tables.shape[0]
            num_batch_request_tables = batch_request_tables.shape[0]
            num_historical_feature_tables = historical_feature_tables.shape[0]
            num_observation_tables = observation_tables.shape[0]

            if num_deployments + num_batch_feature_tables + num_batch_request_tables + num_historical_feature_tables + num_observation_tables > 0:
                if verbose:
                    print("Cleaning catalog: " + catalog_name)

                    if num_deployments > 0:
                        print("  {} deployments".format(num_deployments))
                    if num_batch_feature_tables > 0:
                        print("  {} batch feature tables".format(num_batch_feature_tables))
                    if num_batch_request_tables:
                        print("  {} batch request tables".format(num_batch_request_tables))
                    if num_historical_feature_tables > 0:
                        print("  {} historical feature tables".format(num_historical_feature_tables))
                    if num_observation_tables > 0:
                        print("  {} observation tables".format(num_observation_tables))

                temp_catalog = fb.activate_and_get_catalog(temp_catalog.name)

                for id in deployments.id:
                    deployment = temp_catalog.get_deployment_by_id(id)
                    if deployment.enabled:
                        deployment.disable()

                for id in batch_feature_tables.id:
                    table = temp_catalog.get_batch_feature_table_by_id(id)
                    table.delete()

                for id in batch_request_tables.id:
                    table = temp_catalog.get_batch_request_table_by_id(id)
                    table.delete()

                for id in historical_feature_tables.id:
                    table = temp_catalog.get_historical_feature_table_by_id(id)
                    table.delete()

                for id in observation_tables.id:
                    table = temp_catalog.get_observation_table_by_id(id)
                    table.delete()
                    
                cleaned = True

    if cleaned:
        catalog = fb.activate_and_get_catalog(current_catalog.name)

def register_credit_card_tables():
    # connect to the feature store
    # get data source from the local spark feature store
    ds = fb.FeatureStore.get("playground").get_data_source()

    # get the active catalog
    catalog = fb.Catalog.get_active()

    # check whether the customer data is already registered
    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("BANKCUSTOMER").any():
        BankCustomer = ds.get_source_table(
                    database_name="spark_catalog",
                    schema_name="CREDITCARD",
                    table_name="BANKCUSTOMER"
                ).create_scd_table(
                    name="BANKCUSTOMER",
                        surrogate_key_column='RowID',
                        natural_key_column="BankCustomerID",
                        effective_timestamp_column="ValidFrom",
                        end_timestamp_column="ValidTo",
                        record_creation_timestamp_column="record_available_at"
                )
    else:
        BankCustomer = catalog.get_source_table("BANKCUSTOMER")

    # check whether the state details data is already registered
    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("STATEDETAILS").any():
        StateDetails = ds.get_source_table(
                    database_name="spark_catalog",
                    schema_name="CREDITCARD",
                    table_name="STATEDETAILS"
                ).create_scd_table(
                    name="STATEDETAILS",
                        surrogate_key_column='StateGuid',
                        natural_key_column="StateCode",
                        effective_timestamp_column="ValidFrom" #,
                        #end_timestamp_column="ValidTo" #,
                        #record_creation_timestamp_column="record_available_at"
                )
    else:
        StateDetails = catalog.get_source_table("STATEDETAILS")

    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("CREDITCARD").any():
        CreditCard = ds.get_source_table(
                    database_name="spark_catalog",
                    schema_name="CREDITCARD",
                    table_name="CREDITCARD"
                ).create_scd_table(
                    name="CREDITCARD",
                        surrogate_key_column='RowID',
                        natural_key_column="AccountID",
                        effective_timestamp_column="ValidFrom",
                        end_timestamp_column="ValidTo" #,
                        #record_creation_timestamp_column="record_available_at"
                )
    else:
        CreditCard = catalog.get_source_table("CREDITCARD")

    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("CARDTRANSACTIONS").any():
        # register GroceryInvoice as an event data
        CardTransactions = ds.get_source_table(
                    database_name="spark_catalog",
                    schema_name="CREDITCARD",
                    table_name="CARDTRANSACTIONS"
                ).create_event_table(
                    name="CARDTRANSACTIONS",
                    event_id_column="CardTransactionID",
                    event_timestamp_column="Timestamp",
                    event_timestamp_timezone_offset_column="tz_offset",
                    record_creation_timestamp_column="record_available_at"
                )
    else:
        CardTransactions = catalog.get_source_table("CARDTRANSACTIONS")

    # choose reasonable feature job settings - based upon the feature job analysis
    CardTransactions.update_default_feature_job_setting(
        fb.FeatureJobSetting(
            blind_spot="120s",
            frequency="3600s",
            time_modulo_frequency="65s",
        )
    )

    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("CARDFRAUDSTATUS").any():
        CardFraudStatus = ds.get_source_table(
                    database_name="spark_catalog",
                    schema_name="CREDITCARD",
                    table_name="CARDFRAUDSTATUS"
                ).create_scd_table(
                    name="CARDFRAUDSTATUS",
                        surrogate_key_column='RowID',
                        natural_key_column="CardTransactionID",
                        effective_timestamp_column="ValidFrom",
                        end_timestamp_column="ValidTo",
                        record_creation_timestamp_column="record_available_at"
                )
    else:
        CardFraudStatus = catalog.get_source_table("CARDFRAUDSTATUS")

    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("CARDTRANSACTIONGROUPS").any():
        CardTransactionGroups = ds.get_source_table(
                    database_name="spark_catalog",
                    schema_name="CREDITCARD",
                    table_name="CARDTRANSACTIONGROUPS"
                ).create_dimension_table(
                    name="CARDTRANSACTIONGROUPS",
                    dimension_id_column="CardTransactionDescription"
                )
    else:
        CardTransactionGroups = catalog.get_source_table("CARDTRANSACTIONGROUPS")

    return [BankCustomer, StateDetails, CreditCard, CardTransactions, CardFraudStatus, CardTransactionGroups]

def register_credit_card_entities():
    # register new entities
    entity1 = fb.Entity.get_or_create(name="bank_customer", serving_names=["BANKCUSTOMERID"])
    entity2 = fb.Entity.get_or_create(name="USA_state", serving_names=["STATECODE"])
    entity3 = fb.Entity.get_or_create(name="credit_card", serving_names=["ACCOUNTID"])
    entity4 = fb.Entity.get_or_create(name="card_transaction", serving_names=["CARDTRANSACTIONID"])
    entity5 = fb.Entity.get_or_create(name="card_transaction_description", serving_names=["CARDTRANSACTIONDESCRIPTION"])
    entity6 = fb.Entity.get_or_create(name="gender", serving_names=["GENDER"])
    entity7 = fb.Entity.get_or_create(name="transaction_group", serving_names=["TRANSACTIONGROUP"])

def tag_credit_card_entities_to_columns(bank_customer_table, state_details_table, credit_card_table, card_transaction_table, card_fraud_status_table, card_transaction_group_table):
    # tag the entities for the bank customer table
    bank_customer_table.BankCustomerID.as_entity("bank_customer")
    bank_customer_table.StateCode.as_entity("USA_state")
    bank_customer_table.Gender.as_entity("gender")

    # tag the entities for the state details table
    state_details_table.StateCode.as_entity("USA_state")

    # tag the entities for the credit card table
    credit_card_table.AccountID.as_entity("credit_card")
    credit_card_table.BankCustomerID.as_entity("bank_customer")

    # tag the entities for the card transaction table
    card_transaction_table.CardTransactionID.as_entity("card_transaction")
    card_transaction_table.AccountID.as_entity("credit_card")
    card_transaction_table.CardTransactionDescription.as_entity("card_transaction_description")

    # tag the entities for the card fraud status table
    card_fraud_status_table.CardTransactionID.as_entity("card_transaction")

    # tag the entities for the card transaction group table
    card_transaction_group_table.CardTransactionDescription.as_entity("card_transaction_description")
    card_transaction_group_table.TransactionGroup.as_entity("transaction_group")

def create_playground_credit_card_catalog():
    clean_catalogs(verbose = True)
    
    # generate a new GUID 
    guid = uuid.uuid4()

    # generate a unique catalog name
    catalog_name = "credit card playground " + datetime.now().strftime("%Y%m%d:%H%M") + "_" + str(guid)

    print("Building a playground catalog for credit cards named [" + catalog_name + "]")

    # get a list of catalogs
    catalog_list = fb.Catalog.list()

    # check whether catalog_name exists in the name column of catalog_list
    if catalog_name in catalog_list['name'].values:
        print('Catalog already exists')
    else:
        print('Creating new catalog')
        # creating a catalog activates it
        catalog = fb.Catalog.create(catalog_name, 'playground')
        print('Catalog created')
    catalog = fb.activate_and_get_catalog(catalog_name)        

    print("Registering the source tables")
    [bank_customer_table, state_details_table, credit_card_table, card_transaction_table, card_fraud_status_table, card_transaction_group_table] = register_credit_card_tables()

    print("Registering the entities")
    register_credit_card_entities()

    print("Tagging the entities to columns in the data tables")
    tag_credit_card_entities_to_columns(bank_customer_table, state_details_table, credit_card_table, card_transaction_table, card_fraud_status_table, card_transaction_group_table)

    return catalog
