# Provision a Resource Group

# Import the needed credential and management objects from the libraries.
from azure.mgmt.resource import ResourceManagementClient
from azure.identity import AzureCliCredential
import os

#DAG Libraries
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('azure_container_instances',
         start_date=datetime(2021, 6, 6),
         max_active_runs=1,
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:


        def create_rg():
            # Acquire a credential object using CLI-based authentication.
            credential = AzureCliCredential()
            os.environ["AZURE_SUBSCRIPTION_ID"] = "a2706439-4e8a-4934-ab98-c59ef52ce5b2"
            # Retrieve subscription ID from environment variable.
            subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]

            # Obtain the management object for resources.
            resource_client = ResourceManagementClient(credential, subscription_id)

            # Provision the resource group.
            rg_result = resource_client.resource_groups.create_or_update(
                "PythonAzureExample-rg",
                {
                    "location": "centralus"
                }
            )

            # Within the ResourceManagementClient is an object named resource_groups,
            # which is of class ResourceGroupsOperations, which contains methods like
            # create_or_update.
            #
            # The second parameter to create_or_update here is technically a ResourceGroup
            # object. You can create the object directly using ResourceGroup(location=LOCATION)
            # or you can express the object as inline JSON as shown here. For details,
            # see Inline JSON pattern for object arguments at
            # https://docs.microsoft.com/azure/developer/python/azure-sdk-overview#inline-json-pattern-for-object-arguments.

            print(f"Provisioned resource group {rg_result.name} in the {rg_result.location} region")

            # The return value is another ResourceGroup object with all the details of the
            # new group. In this case the call is synchronous: the resource group has been
            # provisioned by the time the call returns.

            # Update the resource group with tags
            rg_result = resource_client.resource_groups.create_or_update(
                "PythonAzureExample-rg",
                {
                    "location": "centralus",
                    "tags": { "environment":"test", "department":"tech" }
                }
            )

            print(f"Updated resource group {rg_result.name} with tags")

            # Optional lines to delete the resource group. begin_delete is asynchronous.
            # poller = resource_client.resource_groups.begin_delete(rg_result.name)
            # result = poller.result()

        make_rg = PythonOperator(
            task_id='rg',
            python_callable=create_rg
        )
