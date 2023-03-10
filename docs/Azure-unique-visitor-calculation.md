<div id="top"></div>

# Azure SDK provisioning for unique visitor calculation

## Deployment using Azure Portal

## Setup Instructions
<p align="left"><a href="#top">Back to Top</a></p>

This document outlines the steps to configure and deploy Python Azure functions for unique visitor calculation


# what is unique visitor?

# A unique visitor calculated every day based on user agent and client IP is an estimate of the number of distinct individuals who visit a website over a period of time. It is typically calculated by analyzing the user agent and client IP address data that is recorded in the website's server logs

## Prerequisite
<p align="left"><a href="#top">Back to Top</a></p>

1. Should have two container created in Azure storageAccount first one for putting tha data and second one for putting metadata
2. Should have cosmos database created

## Follow below step to setup unique visitor calculation

1. Create one cosmos database container (for example:- data_ingest) that is used for keeping all logline data that is used for calculation of unique visitor
2. Deploy the datastream-sdk in Azure by following [Deployment using Azure Portal Reference](Azure-portal-deployment.md)



# Logic

1. Create a Cosmos DB container with a partition key that includes the date and the last octet of the client IP address. For example, the partition key could be a combination of the date and the last octet of the IP address, such as "20220306_123".
2. Create an Azure function that receives the user-agent and client IP address as input parameters.
3. Use the Azure function to retrieve the current date and the last octet of the client IP address.
4. Use the Cosmos DB SDK to query the Cosmos DB container for documents that match the current date and the last octet of the client IP address.
5. If a document is found, update the document with the user-agent and client IP address if they are not already present in the list of unique visitors for that day.
6. If a document is not found, create a new document with the current date and the last octet of the client IP address as the partition key and the user-agent and client IP address as the first unique visitor for that day.
7. Return a success message from the Azure function.